package banana.dowloader.impl;

import banana.core.modle.BasicWritable;
import banana.core.modle.TaskContext;

public class RemoteTaskContext implements TaskContext {
	
	private final String taskid;
	
	public RemoteTaskContext(String taskid) {
		this.taskid = taskid;
	}

	@Override
	public Object getContextAttribute(String attribute) {
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

	@Override
	public Object putContextAttribute(String attribute, Object value) {
		try {
			DownloadServer.getInstance().getMasterServer().putTaskContextAttribute(taskid, attribute, new BasicWritable(value));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}

	@Override
	public boolean isEmpty() {
		return DownloadServer.getInstance().getMasterServer().taskContextIsEmpty(taskid).get();
	}

}
