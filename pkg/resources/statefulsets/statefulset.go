// Copyright 2018 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statefulsets

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	apps "k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/oracle/mysql-operator/pkg/apis/mysql/v1alpha1"
	"github.com/oracle/mysql-operator/pkg/constants"
	agentopts "github.com/oracle/mysql-operator/pkg/options/agent"
	operatoropts "github.com/oracle/mysql-operator/pkg/options/operator"
	"github.com/oracle/mysql-operator/pkg/resources/secrets"
	"github.com/oracle/mysql-operator/pkg/version"
)

const (
	// MySQLServerName is the static name of all 'mysql(-server)' containers.
	MySQLServerName = "mysql"
	// MySQLAgentName is the static name of all 'mysql-agent' containers.
	MySQLAgentName = "mysql-agent"
	// MySQLAgentBasePath defines the volume mount path for the MySQL agent
	MySQLAgentBasePath = "/var/lib/mysql-agent"

	mySQLBackupVolumeName = "mysqlbackupvolume"
	mySQLVolumeName       = "mysqlvolume"
	mySQLSSLVolumeName    = "mysqlsslvolume"

	replicationGroupPort = 33061 // use default for GCS, eg. seeds in agent ENV
)

func volumeMounts(cluster *v1alpha1.Cluster) []v1.VolumeMount {
	var mounts []v1.VolumeMount

	name := mySQLVolumeName
	if cluster.Spec.VolumeClaimTemplate != nil {
		name = cluster.Spec.VolumeClaimTemplate.Name
	}

	mounts = append(mounts, v1.VolumeMount{
		Name:      name,
		MountPath: "/var/lib/mysql",
		SubPath:   "mysql",
	})

	backupName := mySQLBackupVolumeName
	if cluster.Spec.BackupVolumeClaimTemplate != nil {
		backupName = cluster.Spec.BackupVolumeClaimTemplate.Name
	}
	mounts = append(mounts, v1.VolumeMount{
		Name:      backupName,
		MountPath: MySQLAgentBasePath,
		SubPath:   "mysql",
	})

	// A user may explicitly define a my.cnf configuration file for
	// their MySQL cluster.
	if cluster.RequiresConfigMount() {
		mounts = append(mounts, v1.VolumeMount{
			Name:      cluster.Name,
			MountPath: "/etc/my.cnf",
			SubPath:   "my.cnf",
		})
	}

	if cluster.RequiresCustomSSLSetup() {
		mounts = append(mounts, v1.VolumeMount{
			Name:      mySQLSSLVolumeName,
			MountPath: "/etc/ssl/mysql",
		})
	}

	return mounts
}

func clusterNameEnvVar(cluster *v1alpha1.Cluster) v1.EnvVar {
	return v1.EnvVar{Name: "MYSQL_CLUSTER_NAME", Value: cluster.Name}
}

func namespaceEnvVar() v1.EnvVar {
	return v1.EnvVar{
		Name: "POD_NAMESPACE",
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: "metadata.namespace",
			},
		},
	}
}

func replicationGroupSeedsEnvVar(replicationGroupSeeds string) v1.EnvVar {
	return v1.EnvVar{
		Name:  "REPLICATION_GROUP_SEEDS",
		Value: replicationGroupSeeds,
	}
}

func multiMasterEnvVar(enabled bool) v1.EnvVar {
	return v1.EnvVar{
		Name:  "MYSQL_CLUSTER_MULTI_MASTER",
		Value: strconv.FormatBool(enabled),
	}
}

// Returns the MySQL_ROOT_PASSWORD environment variable
// If a user specifies a secret in the spec we use that
// else we create a secret with a random password
func mysqlRootPassword(cluster *v1alpha1.Cluster) v1.EnvVar {
	var secretName string
	if cluster.RequiresSecret() {
		secretName = secrets.GetRootPasswordSecretName(cluster)
	} else {
		secretName = cluster.Spec.RootPasswordSecret.Name
	}

	return v1.EnvVar{
		Name: "MYSQL_ROOT_PASSWORD",
		ValueFrom: &v1.EnvVarSource{
			SecretKeyRef: &v1.SecretKeySelector{
				LocalObjectReference: v1.LocalObjectReference{
					Name: secretName,
				},
				Key: "password",
			},
		},
	}
}

func getReplicationGroupSeeds(name string, members int) string {
	seeds := []string{}
	for i := 0; i < members; i++ {
		seeds = append(seeds, fmt.Sprintf("%[1]s-%[2]d.%[1]s:%[3]d", name, i, replicationGroupPort))
	}
	return strings.Join(seeds, ",")
}

func precheckContainer(cluster *v1alpha1.Cluster, members int) v1.Container {
	cmd := fmt.Sprintf(`
                replicas=%d
                cluster_name=%s
                is_parallel=%s

                pod_name=$(cat /etc/hostname)
                pod_ordinal=$(cat /etc/hostname | grep -o '[^-]*$')
                seq_num=$(expr $pod_ordinal + 1)

                max_ordinal=$(expr $replicas - 1)
                # check if env is ready for mysql
                # when all hosts pinging ok
                while true
                do
                  env_ready=1

                  if [ $is_parallel = "true" ]; then
                    for i in $(seq 0 $max_ordinal)
                    do
                      ping -c 4 -w 20 $cluster_name-$i.$cluster_name
                      if [ $? -ne 0 ]; then
                        env_ready=0
                        break
                      fi
                    done
                  else
                    ping -c 4 -w 20 $cluster_name-$pod_ordinal.$cluster_name
                    if [ $? -ne 0 ]; then
                      env_ready=0
                    fi
                  fi

                  if [ $env_ready -eq 1 ]; then
                    sleep_seconds=$(expr $seq_num \* 20)
                    max_sleep_seconds=$(expr $replicas \* 20 + 3)
                    sleep_seconds=$(expr $max_sleep_seconds - $sleep_seconds) # the first node sleep most
                    echo "env is ready, waiting ${sleep_seconds} seconds..."
                    sleep $sleep_seconds
                    exit 0
                  else
                    echo "env is not ready, waiting 3 seconds..."
                    sleep 3
                  fi
                done
	`, members, cluster.Name, strconv.FormatBool(cluster.Spec.PodManagementPolicy == apps.ParallelPodManagement))
	return v1.Container{
        Name: "precheck",
        Image: cluster.Spec.PreCheckImage,
		ImagePullPolicy: cluster.Spec.ImagePullPolicy,
		Command:      []string{"/bin/sh", "-cx", cmd},
	}
}

// Builds the MySQL operator container for a cluster.
// The 'mysqlImage' parameter is the image name of the mysql server to use with
// no version information.. e.g. 'mysql/mysql-server'
func mysqlServerContainer(cluster *v1alpha1.Cluster, mysqlServerImage string, rootPassword v1.EnvVar, members int, baseServerID uint32) v1.Container {
	args := []string{
		"--server_id=$(expr $base + $index)",
		"--datadir=/var/lib/mysql",
		"--user=mysql",
		"--gtid_mode=ON",
		"--log-bin=mysql-bin",
		"--binlog_checksum=NONE",
		"--enforce_gtid_consistency=ON",
		"--log-slave-updates=ON",
		"--binlog-format=ROW",
		"--master-info-repository=TABLE",
		"--relay-log-info-repository=TABLE",
		"--transaction-write-set-extraction=XXHASH64",
		"--relay-log=mysql-relay",
		fmt.Sprintf("--report-host=\"%[1]s-${index}.%[1]s\"", cluster.Name),
		"--log-error-verbosity=3",
		fmt.Sprintf("--loose-group-replication-local-address=\"%[1]s-${index}.%[1]s:%[2]d\"", cluster.Name, replicationGroupPort),
		"--loose-group-replication-group-seeds=\"${seeds}\"",
		fmt.Sprintf("--loose-group-replication-group-name=\"%[1]s\"", constants.ReplicationGroupName),
		"--loose-group-replication-start-on-boot=\"OFF\"",
		"--loose-group-replication-bootstrap-group=\"OFF\"",
		"--loose-group-replication-exit-state-action=\"READ_ONLY\"",
		"--loose-group-replication-ip-whitelist=\"0.0.0.0/0\"",
	}

	if cluster.Spec.MultiMaster {
		args = append(args,
			"--loose-group-replication-single-primary-mode=\"OFF\"",
			"--loose-group-replication-enforce-update-everywhere-checks=\"ON\"")
	} else {
		args = append(args,
			"--loose-group-replication-single-primary-mode=\"ON\"",
			"--loose-group-replication-enforce-update-everywhere-checks=\"OFF\"")
	}

	if cluster.RequiresCustomSSLSetup() {
		args = append(args,
			"--ssl-ca=/etc/ssl/mysql/ca.crt",
			"--ssl-cert=/etc/ssl/mysql/tls.crt",
			"--ssl-key=/etc/ssl/mysql/tls.key")
	}

	entryPointArgs := strings.Join(args, " ")

	cmd := fmt.Sprintf(`
         # Set baseServerID
         base=%d

         # Finds the replica index from the hostname, and uses this to define
         # a unique server id for this instance.
         index=$(cat /etc/hostname | grep -o '[^-]*$')

         # set seeds to other nodes
         members=%d
         cluster_name=%s
         repl_group_port=%d
         max_index=$(expr $members - 1)
         seeds=""
         for i in $(seq 0 $max_index)
         do
           if [ $i -eq $index ]; then
             continue;
           fi
           if [ -z $seeds ]; then
             seeds=${cluster_name}-${i}.${cluster_name}:${repl_group_port}
           else
             seeds=${seeds},${cluster_name}-${i}.${cluster_name}:${repl_group_port}
           fi
         done         
         /entrypoint.sh %s`, baseServerID, members, cluster.Name, replicationGroupPort, entryPointArgs)
	return v1.Container{
		Name: MySQLServerName,
		// TODO(apryde): Add BaseImage to cluster CRD.
		Image: fmt.Sprintf("%s:%s", mysqlServerImage, cluster.Spec.Version),
		ImagePullPolicy: cluster.Spec.ImagePullPolicy,
		Ports: []v1.ContainerPort{
			{
				ContainerPort: 3306,
			},
		},
		VolumeMounts: volumeMounts(cluster),
		Command:      []string{"/bin/bash", "-ecx", cmd},
		Env: []v1.EnvVar{
			rootPassword,
			{
				Name:  "MYSQL_ROOT_HOST",
				Value: "%",
			},
			{
				Name:  "MYSQL_LOG_CONSOLE",
				Value: "true",
			},
		},
	}
}

func mysqlAgentContainer(cluster *v1alpha1.Cluster, mysqlAgentImage string, rootPassword v1.EnvVar, members int) v1.Container {
	agentVersion := version.GetBuildVersion()
	if version := os.Getenv("MYSQL_AGENT_VERSION"); version != "" {
		agentVersion = version
	}

	replicationGroupSeeds := getReplicationGroupSeeds(cluster.Name, members)

	return v1.Container{
		Name:         MySQLAgentName,
		Image:        fmt.Sprintf("%s:%s", mysqlAgentImage, agentVersion),
		ImagePullPolicy: cluster.Spec.ImagePullPolicy,
		Args:         []string{fmt.Sprintf("--v=%d", cluster.Spec.LogLevel)},
		VolumeMounts: volumeMounts(cluster),
		Env: []v1.EnvVar{
			clusterNameEnvVar(cluster),
			namespaceEnvVar(),
			replicationGroupSeedsEnvVar(replicationGroupSeeds),
			multiMasterEnvVar(cluster.Spec.MultiMaster),
			{
				Name:  "POD_MANAGEMENT_POLICY",
				Value: string(cluster.Spec.PodManagementPolicy),
			},
			rootPassword,
			{
				Name: "MY_POD_IP",
				ValueFrom: &v1.EnvVarSource{
					FieldRef: &v1.ObjectFieldSelector{
						FieldPath: "status.podIP",
					},
				},
			},
		},
		LivenessProbe: &v1.Probe{
			Handler: v1.Handler{
				HTTPGet: &v1.HTTPGetAction{
					Path: "/live",
					Port: intstr.FromInt(int(agentopts.DefaultMySQLAgentHeathcheckPort)),
				},
			},
		},
		ReadinessProbe: &v1.Probe{
			Handler: v1.Handler{
				HTTPGet: &v1.HTTPGetAction{
					Path: "/ready",
					Port: intstr.FromInt(int(agentopts.DefaultMySQLAgentHeathcheckPort)),
				},
			},
		},
	}
}

func exporterContainer(cluster *v1alpha1.Cluster, rootPassword v1.EnvVar) v1.Container {
	if cluster.Spec.ExporterImage == "" {
		return v1.Container{}
	} else {
		return v1.Container{
            Name: "exporter",
            Image: cluster.Spec.ExporterImage,
			Env: []v1.EnvVar{
				rootPassword,
				{
					Name:  "DATA_SOURCE_NAME",
					Value: "root:${MYSQL_ROOT_PASSWORD}@(127.0.0.1:3306)/",
				},
			},
			Ports: []v1.ContainerPort{
				{
					ContainerPort: 9104,
				},
			},
		}
	}
}

// NewForCluster creates a new StatefulSet for the given Cluster.
func NewForCluster(cluster *v1alpha1.Cluster, images operatoropts.Images, serviceName string) *apps.StatefulSet {
	rootPassword := mysqlRootPassword(cluster)
	members := int(cluster.Spec.Members)
	baseServerID := cluster.Spec.BaseServerID

	// If a PV isn't specified just use a EmptyDir volume
	var podVolumes = []v1.Volume{}
	if cluster.Spec.VolumeClaimTemplate == nil {
		podVolumes = append(podVolumes, v1.Volume{Name: mySQLVolumeName,
			VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{Medium: ""}}})
	}

	// If a Backup PV isn't specified just use a EmptyDir volume
	if cluster.Spec.BackupVolumeClaimTemplate == nil {
		podVolumes = append(podVolumes, v1.Volume{Name: mySQLBackupVolumeName,
			VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{Medium: ""}}})
	}

	if cluster.RequiresConfigMount() {
		podVolumes = append(podVolumes, v1.Volume{
			Name: cluster.Name,
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: cluster.Spec.Config.Name,
					},
				},
			},
		})
	}

	if cluster.RequiresCustomSSLSetup() {
		podVolumes = append(podVolumes, v1.Volume{
			Name: mySQLSSLVolumeName,
			VolumeSource: v1.VolumeSource{
				Projected: &v1.ProjectedVolumeSource{
					Sources: []v1.VolumeProjection{
						{
							Secret: &v1.SecretProjection{
								LocalObjectReference: v1.LocalObjectReference{
									Name: cluster.Spec.SSLSecret.Name,
								},
								Items: []v1.KeyToPath{
									{
										Key:  "ca.crt",
										Path: "ca.crt",
									},
									{
										Key:  "tls.crt",
										Path: "tls.crt",
									},
									{
										Key:  "tls.key",
										Path: "tls.key",
									},
								},
							},
						},
					},
				},
			},
		})
	}

	var initContainers []v1.Container
	if cluster.Spec.InitContainers == nil ||
		len(cluster.Spec.InitContainers) == 0 {
		initContainers = []v1.Container{
			precheckContainer(cluster, members),
		}
	} else {
		initContainers = cluster.Spec.InitContainers
	}

	containers := []v1.Container{
		mysqlServerContainer(cluster, images.MySQLServerImage, rootPassword, members, baseServerID),
		mysqlAgentContainer(cluster, images.MySQLAgentImage, rootPassword, members)}
	if cluster.Spec.ExporterImage != "" {
		containers = append(containers, exporterContainer(cluster, rootPassword))
	}

	podLabels := map[string]string{
		constants.ClusterLabel: cluster.Name,
		//constants.ReplicasLabel: strconv.Itoa(members),
	}
	if cluster.Spec.MultiMaster {
		podLabels[constants.LabelClusterRole] = constants.ClusterRolePrimary
	}

	ss := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Name:      cluster.Name,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, schema.GroupVersionKind{
					Group:   v1alpha1.SchemeGroupVersion.Group,
					Version: v1alpha1.SchemeGroupVersion.Version,
					Kind:    v1alpha1.ClusterCRDResourceKind,
				}),
			},
			Labels: map[string]string{
				constants.ClusterLabel:              cluster.Name,
				constants.MySQLOperatorVersionLabel: version.GetBuildVersion(),
			},
		},
		Spec: apps.StatefulSetSpec{
			PodManagementPolicy: cluster.Spec.PodManagementPolicy,
			Replicas: &cluster.Spec.Members,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels,
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "8080",
					},
				},
				Spec: v1.PodSpec{
					// FIXME: LIMITED TO DEFAULT NAMESPACE. Need to dynamically
					// create service accounts and (cluster role bindings?)
					// for each namespace.
					ServiceAccountName: "mysql-agent",
					NodeSelector:       cluster.Spec.NodeSelector,
					Affinity:           cluster.Spec.Affinity,
					InitContainers:     initContainers,
					Containers:         containers,
					Volumes:            podVolumes,
				},
			},
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.RollingUpdateStatefulSetStrategyType,
			},
			ServiceName: serviceName,
		},
	}

	if cluster.Spec.VolumeClaimTemplate != nil {
		ss.Spec.VolumeClaimTemplates = append(ss.Spec.VolumeClaimTemplates, *cluster.Spec.VolumeClaimTemplate)
	}
	if cluster.Spec.BackupVolumeClaimTemplate != nil {
		ss.Spec.VolumeClaimTemplates = append(ss.Spec.VolumeClaimTemplates, *cluster.Spec.BackupVolumeClaimTemplate)
	}
	return ss
}
