package LocalApp;
import Util.InstanceState;

import java.util.HashMap;


public class InstanceStatesDictionary {

	public static final HashMap<Integer, InstanceState> instanceStatesDictionary;
	static
	{
		instanceStatesDictionary = new HashMap<Integer, InstanceState>();
		instanceStatesDictionary.put(0, InstanceState.Pending);
		instanceStatesDictionary.put(16, InstanceState.Running);
		instanceStatesDictionary.put(32, InstanceState.ShuttingDown);
		instanceStatesDictionary.put(48, InstanceState.Terminated);
		instanceStatesDictionary.put(64, InstanceState.Stopping);
		instanceStatesDictionary.put(80, InstanceState.Stopped);
	}
}

