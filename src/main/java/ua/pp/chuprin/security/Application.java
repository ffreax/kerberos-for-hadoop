package ua.pp.chuprin.security;

import com.jcraft.jsch.*;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;
import ua.pp.chuprin.security.hadoop.Cluster;
import ua.pp.chuprin.security.hadoop.Daemon;
import ua.pp.chuprin.security.hadoop.DaemonType;
import ua.pp.chuprin.security.hadoop.Node;
import ua.pp.chuprin.security.ssh.UserIdentity;

import java.io.*;

public class Application {

	private static FileWriter log = null;
	private static final XStream XSTREAM = new XStream(new StaxDriver());

	public static void main(String[] args) throws IOException, InterruptedException, JSchException, SftpException {
		Session kerberos = null;
		try {
			log = new FileWriter("runtime.log");

			Cluster cluster = readClusterInfo();
			kerberos = connect(cluster.kerberosSsh, cluster.kerberosHostName, cluster.kerberosSshPort);
			String coreSite = ConfigHelper.buildCoreSite();
			for (Node node : cluster.nodes) {
				Session session = null;
				try {
					session = connect(node.sshUserIdentity, node.hostName, node.sshPort);

					String dataNode = "";
					String taskTracker = "";
					for (Daemon daemon : node.daemons) {
						createPrincipal(kerberos, daemon);
						createKeyTab(kerberos, daemon);
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
		} finally {
			if(log != null) {
				log.close();
			}
			if(kerberos != null) {
				kerberos.disconnect();
			}
		}
	}

	private static void transferKeyTab(Session session, Daemon daemon) throws JSchException, SftpException, IOException {
		execRemote(session, "mkdir -p " + daemon.getNode().keyTab);

		ChannelSftp channel = (ChannelSftp) session.openChannel("sftp");
		channel.connect();
		channel.put(getPrincipalKeyTab(daemon), getPrincipalFullKeyTab(daemon));
		new File(getPrincipalKeyTab(daemon)).delete();

		execRemote(session, "chown " + daemon.getNode().hadoopUser + " " + getPrincipalFullKeyTab(daemon));
		execRemote(session, "chmod 400 " + getPrincipalFullKeyTab(daemon));
	}

	private static Session connect(UserIdentity sshUserIdentity, String hostName, int sshPort) throws JSchException {
		JSch jsch = new JSch();
		jsch.setKnownHosts("known_hosts.txt");
		byte[] passphrase;
		if (sshUserIdentity.passphrase == null) {
			passphrase = null;
		} else {
			passphrase = sshUserIdentity.passphrase.getBytes();
		}
		jsch.addIdentity(sshUserIdentity.privateKeyPath, sshUserIdentity.publicKeyPath, passphrase);

		Session session = jsch.getSession(sshUserIdentity.user, hostName, sshPort);
		session.setConfig("StrictHostKeyChecking", "no");
		session.connect();

		return session;
	}

	private static void appendConfig(Node node, Session session, String file, String properties) throws IOException, JSchException {
		String oldConfig = execRemote(session, "cat " + node.conf + "/" + file);
		String newConfig = oldConfig.replace("</configuration>", properties + "\n</configuration>");
		execRemote(session, "echo '" + newConfig+ "' > " + node.conf + "/" + file);
	}

	private static String execRemote(Session session, String command) throws JSchException, IOException {
		log.write(session.getHost() + " >> : " + command + "\n");

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
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}

			String response = result.toString().trim();

			log.write(session.getHost() + " << : " + response + "\n");
			return response;
		} finally {
			connection.disconnect();
		}
	}

	private static void createKeyTab(Session kerberos, Daemon daemon) throws IOException, InterruptedException, JSchException, SftpException {
		String principalKeyTab = getPrincipalKeyTab(daemon);

		execRemote(kerberos, kadminQuery(daemon.getNode().getCluster(),
				"xst -k " + principalKeyTab + " " + getPrincipalFullName(daemon)));

		ChannelSftp channel = (ChannelSftp) kerberos.openChannel("sftp");
		channel.connect();
		channel.get(principalKeyTab, principalKeyTab);
		channel.rm(principalKeyTab);
	}

	private static String kadminQuery(Cluster cluster, String query) {
		return "kadmin.local -r " + cluster.realm +
				" -q \"" + query + "\"";
	}

	private static void createPrincipal(Session kerberos, Daemon daemon) throws IOException, InterruptedException, JSchException {
		execRemote(kerberos, kadminQuery(daemon.getNode().getCluster(),
				"addprinc -randkey " + getPrincipalFullName(daemon)));
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
		log.write("localhost >> : " + command + "\n");
		Process process = Runtime.getRuntime().exec(command);

		String result = readStream(process.getInputStream());
		log.write("localhost << : " + result + "\n");

		int returnCode = process.waitFor();

		if(returnCode != 0) {
			log.write("localhost <<:err : " + readStream(process.getErrorStream()) + "\n");

			throw new IllegalStateException("Error (" + returnCode + ") while execute: " + command);
		}

		return result;
	}

	private static String readStream(InputStream inputStream) throws IOException {
		BufferedReader in = null;
		String result;
		try {
			InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
			in = new BufferedReader(inputStreamReader);

			StringBuffer buffer = new StringBuffer();
			String line;
			while ((line = in.readLine()) != null) {
				buffer.append(line);
			}

			result = buffer.toString();
		} finally {
			if(in != null) {
				in.close();
			}
		}
		return result;
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
		Cluster cluster = (Cluster) XSTREAM.fromXML(new File("cluster.xml"));
		resolveBidirectionals(cluster);

		return cluster;
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
