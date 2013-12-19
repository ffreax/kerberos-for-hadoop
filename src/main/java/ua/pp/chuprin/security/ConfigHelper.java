package ua.pp.chuprin.security;

import ua.pp.chuprin.security.hadoop.Cluster;
import ua.pp.chuprin.security.hadoop.Daemon;


public class ConfigHelper {
	public static String buildMapredSite(Cluster cluster, String taskTracker) {
		Daemon jobTracker = cluster.getJobTracker();

		return "<property>\n" +
				"\t\t<name>mapreduce.jobtracker.kerberos.principal</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullName(jobTracker) + "</value>\n" +
				"\t\t<description>Kerberos principal name for the JobTracker</description>\n" +
				"\t</property>\n" +
				"\t<property>\n" +
				"\t\t<name>mapreduce.jobtracker.keytab.file</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullKeyTab(jobTracker) + "</value>\n" +
				"\t\t<description>The keytab for the JobTracker principal.</description>\n" +
				"\t</property>\n" +
				"\n" +
				"\t<property>\n" +
				"\t\t<name>mapreduce.jobhistory.kerberos.principal</name>\n" +
				"\t\t<!--cluster variant -->\n" +
				"\t\t<value>" + Application.getPrincipalFullName(jobTracker) + "</value>\n" +
				"\t\t<description>Kerberos principal name for JobHistory. This must map to the same user as the JobTracker user\n" +
				"\t\t\t(mapred). </description>\n" +
				"\t</property>\n" +
				"\t<property>\n" +
				"\t\t<name>mapreduce.jobhistory.keytab.file</name>\n" +
				"\t\t<!--cluster variant -->\n" +
				"\t\t<value>" + Application.getPrincipalFullKeyTab(jobTracker) + "</value>\n" +
				"\t\t<description>The keytab for the JobHistory principal.</description>\n" +
				"\t</property>\n" +
				"\n" +
				taskTracker;
	}

	public static String buildTaskTracker(Daemon taskTracker) {
		return 	"\t<property>\n" +
				"\t\t<name>mapreduce.tasktracker.kerberos.principal</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullName(taskTracker) + "</value>\n" +
				"\t\t<description>Kerberos principal name for the TaskTracker.</description>\n" +
				"\t</property>\n" +
				"\t<property>\n" +
				"\t\t<name>mapreduce.tasktracker.keytab.file</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullKeyTab(taskTracker) + "</value>\n" +
				"\t\t<description>The filename of the keytab for the TaskTracker</description>\n" +
				"\t</property>";
	}

	public static String buildHdfsSite(Cluster cluster, String dataNode) {
		Daemon nameNode = cluster.getNameNode();
		Daemon secondaryNameNode = cluster.getSecondaryNameNode();

		return "\t<property>\n" +
				"\t\t<name>dfs.block.access.token.enable</name>\n" +
				"\t\t<value>true</value>\n" +
				"\t\t<description>If \"true\", access tokens are used as capabilities\n" +
				"\t\t\tfor accessing datanodes. If \"false\", no access tokens are checked on\n" +
				"\t\t\taccessing datanodes. </description>\n" +
				"\t</property>\n" +
				"\n" +
				"\t<property>\n" +
				"\t\t<name>dfs.namenode.kerberos.principal</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullName(nameNode) + "</value>\n" +
				"\t\t<description>Kerberos principal name for the NameNode</description>\n" +
				"\t</property>\n" +
				"\t<property>\n" +
				"\t\t<name>dfs.namenode.keytab.file</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullKeyTab(nameNode) + "</value>\n" +
				"\t\t<description>Combined keytab file containing the NameNode service and host principals.</description>\n" +
				"\t</property>\n" +
				"\n" +
				"\t<property>\n" +
				"\t\t<name>dfs.secondary.namenode.kerberos.principal</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullName(secondaryNameNode) + "</value>\n" +
				"\t\t<description>Kerberos principal name for the secondary NameNode.</description>\n" +
				"\t</property>\n" +
				"\t<property>\n" +
				"\t\t<name>dfs.secondary.namenode.keytab.file</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullKeyTab(secondaryNameNode) + "</value>\n" +
				"\t\t<description>Combined keytab file containing the NameNode service and host principals.</description>\n" +
				"\t</property>\n" +
				"\n" +
				dataNode;
	}

	public static String buildDataNode(Daemon dataNode) {
		return "\t<property>\n" +
				"\t\t<name>dfs.datanode.kerberos.principal</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullName(dataNode) + "</value>\n" +
				"\t\t<description>The Kerberos principal that the DataNode runs as.</description>\n" +
				"\t</property>\n" +
				"\t<property>\n" +
				"\t\t<name>dfs.datanode.keytab.file</name>\n" +
				"\t\t<value>" + Application.getPrincipalFullKeyTab(dataNode) + "</value>\n" +
				"\t\t<description>The filename of the keytab file for the DataNode.</description>\n" +
				"\t</property>\n";
	}

	public static String buildCoreSite() {
		return "\t<property>\n" +
				"\t\t<name>hadoop.security.authentication</name>\n" +
				"\t\t<value>kerberos</value>\n" +
				"\t\t<description>Set the authentication for the cluster. Valid values are: simple or kerberos.</description>\n" +
				"\t</property>\n" +
				"\t<property>\n" +
				"\t\t<name>hadoop.rpc.protection</name>\n" +
				"\t\t<value>authentication</value>\n" +
				"\t\t<description>This is an [OPTIONAL] setting. If not set, defaults to authentication.authentication=\n" +
				"\t\t\tauthentication only; the client and server mutually authenticate during connection setup.integrity =\n" +
				"\t\t\tauthentication and integrity; guarantees the integrity of data exchanged between client and server aswell as\n" +
				"\t\t\tauthentication.privacy = authentication, integrity, and confidentiality; guarantees that data exchanged\n" +
				"\t\t\tbetween client andserver is encrypted and is not readable by a “man in the middle”. </description>\n" +
				"\t</property>\n" +
				"\t<property>\n" +
				"\t\t<name>hadoop.security.auth_to_local</name>\n" +
				"\t\t<value>hduser</value>\n" +
				"\t\t<description>The mapping from Kerberos principal names to local OS user names.</description>\n" +
				"\t</property>";
	}
}
