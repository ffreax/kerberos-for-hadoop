package ua.pp.chuprin.security.hadoop;

import ua.pp.chuprin.security.ssh.UserIdentity;

import java.util.ArrayList;
import java.util.List;

public class Cluster {
	public final List<Node> nodes;

	public final UserIdentity kerberosSsh;
	public final String realm;
	public final String rootPrincipalName;
	public final String rootPrincipalPassword;
	public final String kerberosHostName;
	public final int kerberosSshPort;

	public Cluster(List<Node> nodes, UserIdentity kerberosSsh, String realm,
	               String rootPrincipal, String rootPassword, String kerberosHostName, int kerberosSshPort) {
		this.nodes = nodes;
		this.kerberosSsh = kerberosSsh;
		this.realm = realm;
		this.rootPrincipalName = rootPrincipal;
		this.rootPrincipalPassword = rootPassword;
		this.kerberosHostName = kerberosHostName;
		this.kerberosSshPort = kerberosSshPort;
	}

	public Daemon getNameNode() {
		return getByType(DaemonType.NAME_NODE).get(0);
	}

	public Daemon getSecondaryNameNode() {
		return getByType(DaemonType.SECONDARY_NAME_NODE).get(0);
	}

	public Daemon getJobTracker() {
		return getByType(DaemonType.JOB_TRACKER).get(0);
	}

	private List<Daemon> getByType(DaemonType requestType) {
		List<Daemon> result = new ArrayList<Daemon>();

		for (Node node : nodes) {
			for (Daemon daemon : node.daemons) {
				if(daemon.type == requestType) {
					result.add(daemon);
				}
			}
		}

		return result;
	}
}
