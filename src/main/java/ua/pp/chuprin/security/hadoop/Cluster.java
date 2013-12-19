package ua.pp.chuprin.security.hadoop;

import java.util.ArrayList;
import java.util.List;

public class Cluster {
	public final List<Node> nodes;
	public final String realm;
	public final String rootPrincipal;
	public final String rootPassword;

	public Cluster(List<Node> nodes, String realm, String rootPrincipal, String rootPassword) {
		this.nodes = nodes;
		this.realm = realm;
		this.rootPrincipal = rootPrincipal;
		this.rootPassword = rootPassword;
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
