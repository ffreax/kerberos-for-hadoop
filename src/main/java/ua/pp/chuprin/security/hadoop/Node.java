package ua.pp.chuprin.security.hadoop;

import ua.pp.chuprin.security.ssh.User;

import java.util.List;

public class Node {

	public final String hostName;
	public final List<Daemon> daemons;
	public final User sshUser;
	public final int sshPort;

	public final String conf;
	public final String keyTab;
	public final String hadoopUser;

	private Cluster cluster;

	public Node(String hostName, List<Daemon> daemons, User sshUser, int sshPort, String conf, String keyTab, String hadoopUser) {
		this.hostName = hostName;
		this.sshUser = sshUser;
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
