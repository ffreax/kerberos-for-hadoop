package ua.pp.chuprin.security;

import com.jcraft.jsch.*;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;
import ua.pp.chuprin.security.hadoop.Cluster;
import ua.pp.chuprin.security.hadoop.Daemon;
import ua.pp.chuprin.security.hadoop.DaemonType;
import ua.pp.chuprin.security.hadoop.Node;

import java.io.*;

public class Application {

	private static Session kerb;

	public static void main(String[] args) throws IOException, InterruptedException, JSchException, SftpException {
		Cluster cluster = readClusterInfo();
		String coreSite = ConfigHelper.buildCoreSite();
		for (Node node : cluster.nodes) {
			Session session = null;
			try {
				session = connect(node);

				String dataNode = "";
				String taskTracker = "";
				for (Daemon daemon : node.daemons) {
					createPrincipal(daemon);
					createKeyTab(daemon);
					transferKeyTab(session, daemon);

					if(daemon.type == DaemonType.TASK_TRACKER) {
						taskTracker = ConfigHelper.buildTaskTracker(daemon);
					} else if(daemon.type == DaemonType.DATA_NODE) {
						dataNode = ConfigHelper.buildDataNode(daemon);
					}
				}
				appendConfig(node, session, "core-site.xml", coreSite);

				appendConfig(node, session, "hdfs-site.xml", ConfigHelper.buildHdfsSite(cluster, dataNode));

				appendConfig(node, session, "mapred-site.xml", ConfigHelper.buildMapredSite(cluster, taskTracker));
			} finally {
				if(session != null) {
					session.disconnect();
				}
			}
		}
	}

	private static void transferKeyTab(Session session, Daemon daemon) throws JSchException, SftpException, IOException {
		ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
		channel.connect();
		channel.put(getPrincipalKeyTab(daemon), getPrincipalFullKeyTab(daemon));

		execRemote(session, "chown " + daemon.getNode().hadoopUser + " " + getPrincipalFullKeyTab(daemon));
		execRemote(session, "chmod 400 " + getPrincipalFullKeyTab(daemon));
	}

	private static Session connect(Node node) throws JSchException {
		JSch jsch = new JSch();
		jsch.setKnownHosts("known_hosts.txt");
		byte[] passphrase;
		if (node.sshUser.passphrase == null) {
			passphrase = null;
		} else {
			passphrase = node.sshUser.passphrase.getBytes();
		}
		jsch.addIdentity(node.sshUser.privateKeyPath, node.sshUser.publicKeyPath, passphrase);

		Session session = jsch.getSession(node.sshUser.user, node.hostName, node.sshPort);
		session.setConfig("StrictHostKeyChecking", "no");
		session.connect();

		return session;
	}

	private static void appendConfig(Node node, Session session, String file, String properties) throws IOException, JSchException {
		String oldConfig = execRemote(session, "cat " + node.conf + "/" + file);
		String newConfig = oldConfig.replace("</configuration>", properties + "</configuration>");
		execRemote(session, "echo \"" + newConfig+ "\" > " + node.conf + "/" + file);
	}

	private static String execRemote(Session session, String command) throws JSchException, IOException {
		ChannelExec connection = null;
		try {

			connection = (ChannelExec) session.openChannel("exec");

			connection.setCommand(command);

			InputStream in = connection.getInputStream();

			connection.connect();

			StringBuilder result = new StringBuilder();
			while (true) {


				byte[] buffer = new byte[1024];
				while (in.available() > 0) {
					int i = in.read(buffer, 0, 1024);
					if (i < 0) {
						break;
					}

					result.append(new String(buffer, 0, i));
				}

				if (connection.isClosed()) {
					System.out.println("exit-status: " + connection.getExitStatus());
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}

			return result.toString().trim();
		} finally {
			connection.disconnect();
		}
	}

	private static void createKeyTab(Daemon daemon) throws IOException, InterruptedException {
		execLocal("kadmin.local -q \"xst -k " + getPrincipalKeyTab(daemon) + " " + getPrincipalFullName(daemon) + "\"");
	}

	private static void createPrincipal(Daemon daemon) throws IOException, InterruptedException {
		execLocal("kadmin.local -q \"addprinc -randkey " + getPrincipalFullName(daemon) + "\"");
	}

	public static String getPrincipalFullKeyTab(Daemon daemon) {
		return daemon.getNode().keyTab + "/" + getPrincipalKeyTab(daemon);
	}

	public static String getPrincipalKeyTab(Daemon daemon) {
		return principalName(daemon.type) + ".service.keytab";
	}

	public static String getPrincipalFullName(Daemon daemon) {
		return principalName(daemon.type) + "/" + daemon.getNode().hostName + "@" + daemon.getNode().getCluster().realm;
	}

	private static String execLocal(String command) throws IOException, InterruptedException {
		Process process = Runtime.getRuntime().exec(command);
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(process.getInputStream()));

			StringBuffer buffer = new StringBuffer();
			String line;
			while ((line = in.readLine()) != null) {
				buffer.append(line);
			}
			int returnCode = process.waitFor();
			if(returnCode != 0) {
				throw new IllegalStateException("Error while execute: " + command);
			}

			return buffer.toString();
		} finally {
			if(in != null) {
				in.close();
			}
		}
	}

	private static String principalName(DaemonType type) {
		switch (type) {
			case NAME_NODE:
				return "nn";
			case SECONDARY_NAME_NODE:
				return "snn";
			case JOB_TRACKER:
				return "jt";
			case TASK_TRACKER:
				return "tt";
			case DATA_NODE:
				return "dn";
			default:
				return "unknown";
		}
	}

	private static Cluster readClusterInfo() {
//		Daemon nameDaemon = new Daemon(DaemonType.NAME_NODE);
//		Daemon taskDaemon = new Daemon(DaemonType.TASK_TRACKER);
//		Node master = new Node("master.hadoop.lan", new ArrayList<Daemon>(Arrays.asList(nameDaemon, taskDaemon)),
//				new User("hduser",
//					"d:/Google Диск/ВУЗ/диплом/clusters/local/hadoop-master-hduser.pub.key",
//					"d:/Google Диск/ВУЗ/диплом/clusters/local/hadoop-master-hduser.key"),
//				22, "/opt/hadoop/conf", "/etc/security/keytabs", "hduser:hadoop");
//		nameDaemon.setNode(master);
//		taskDaemon.setNode(master);
//
//		Daemon secondaryNameDaemon = new Daemon(DaemonType.SECONDARY_NAME_NODE);
//		Node backup = new Node("master.hadoop.lan", new ArrayList<Daemon>(Arrays.asList(secondaryNameDaemon)),
//				new User("hduser",
//						"d:/Google Диск/ВУЗ/диплом/clusters/local/hadoop-backup-hduser.pub.key",
//						"d:/Google Диск/ВУЗ/диплом/clusters/local/hadoop-backup-hduser.key"),
//				22, "/opt/hadoop/conf", "/etc/security/keytabs", "hduser:hadoop");
//		secondaryNameDaemon.setNode(backup);
//
//		Daemon jobTracker1 = new Daemon(DaemonType.JOB_TRACKER);
//		Daemon dataTracker1 = new Daemon(DaemonType.DATA_NODE);
//		Node hadoop1 = new Node("master.hadoop.lan", new ArrayList<Daemon>(Arrays.asList(jobTracker1, dataTracker1)),
//				new User("hduser",
//						"d:/Google Диск/ВУЗ/диплом/clusters/local/hadoop-hadoop1-hduser.pub.key",
//						"d:/Google Диск/ВУЗ/диплом/clusters/local/hadoop-hadoop1-hduser.key"),
//				22, "/opt/hadoop/conf", "/etc/security/keytabs", "hduser:hadoop");
//		jobTracker1.setNode(hadoop1);
//		dataTracker1.setNode(hadoop1);
//
//		Cluster cluster = new Cluster(new ArrayList<Node>(Arrays.asList(master, backup, hadoop1)), "HADOOP.LAN");
//		master.setCluster(cluster);
//		backup.setCluster(cluster);
//		hadoop1.setCluster(cluster);
//
//		String xml = xstream.toXML(cluster);

		XStream xstream = new XStream(new StaxDriver());
		Cluster cluster2 = (Cluster) xstream.fromXML(new File("D:\\projects\\kerberos-for-hadoop\\settigs.xml"));
		resolveBidirectionals(cluster2);

		try {
			kerb = connect(cluster2.getNameNode().getNode());
			String result = execRemote(kerb, "echo \"test\"");
			String result2 = execLocal("ls -l");
		} catch (JSchException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		} catch (InterruptedException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		}

		return cluster2;
	}

	private static void resolveBidirectionals(Cluster cluster) {
		for (Node node : cluster.nodes) {
			node.setCluster(cluster);
			for (Daemon daemon : node.daemons) {
				daemon.setNode(node);
			}
		}
	}
}
