package ua.pp.chuprin.security.hadoop;

public class Daemon {

	public final DaemonType type;

	private Node node;

	public Daemon(DaemonType type) {
		this.type = type;
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}
}
