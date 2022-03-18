package java_hadoop.chapter21;

import java.util.List;

import org.apache.zookeeper.KeeperException;

public class ListGroup extends ConnectionWatcher {
	
	public void list(String groupName) throws KeeperException, InterruptedException {
		String path = "/" + groupName;
		
		List<String> children = zk.getChildren(path, false);
		if (children.isEmpty()) {
			System.out.printf("No members in group %s\n", groupName);
			return;
		}
		
		System.out.printf("There are %d members in group %s\n", children.size(), groupName);
		for (String child : children) {
			System.out.println(child);
		}
	}

	public static void main(String[] args) throws Exception {
		ListGroup listGroup = new ListGroup();
		listGroup.connect("localhost:2181");
		listGroup.list("zoo");
		listGroup.close();
	}
}
