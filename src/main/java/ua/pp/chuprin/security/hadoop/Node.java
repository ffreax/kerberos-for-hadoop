package ua.pp.chuprin.security.hadoop;

import ua.pp.chuprin.security.ssh.UserIdentity;

import java.util.List;

public class Node {

	public final List<Daemon> daemons;
	public final String hostName;
	public final UserIdentity sshUserIdentity;
	public final int sshPort;

	public final String conf;
	public final String keyTab;
	public final String hadoopUser;

	private Cluster cluster;

	public Node(String hostName, List<Daemon> daemons, UserIdentity sshUserIdentity, int sshPort, String conf, String keyTab, String hadoopUser) {
		this.hostName = hostName;
		this.sshUserIdentity = sshUserIdentity;
		this.daemons = daemons;
		this.sshPort = sshPort;
		this.conf = conf;
		this.keyTab = keyTab;
		this.hadoopUser = hadoopUser;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}
}
