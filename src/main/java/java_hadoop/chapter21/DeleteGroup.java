package java_hadoop.chapter21;

import java.util.List;

import org.apache.zookeeper.KeeperException;

public class DeleteGroup extends ConnectionWatcher {

	public void delete(String groupName) throws KeeperException, InterruptedException {
		String path = "/" + groupName;
		
		List<String> children = zk.getChildren(path, false);
		for (String child : children) {
			zk.delete(String.format("%s/%s", path, child), -1);
		}
	}

	public static void main(String[] args) throws Exception {
		DeleteGroup deleteGroup = new DeleteGroup();
		deleteGroup.connect("localhost:2181");
		deleteGroup.delete("zoo");
		deleteGroup.close();
	}
}
