package banana.dowloader.impl;

import banana.core.modle.BasicWritable;

public class RemoteTaskContext{
	
	private final String taskid;
	
	public RemoteTaskContext(String taskid) {
		this.taskid = taskid;
	}

	public Object get(String attribute) {
		try {
			BasicWritable value = DownloadServer.getInstance().getMasterServer().getTaskContextAttribute(taskid, attribute);
			if (value != null){
				return value.getValue();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public Object put(String attribute, Object value) {
		try {
			DownloadServer.getInstance().getMasterServer().putTaskContextAttribute(taskid, attribute, new BasicWritable(value));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}

}
