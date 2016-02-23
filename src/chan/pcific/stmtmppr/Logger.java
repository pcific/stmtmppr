package chan.pcific.stmtmppr;

import java.text.SimpleDateFormat;
import java.util.Date;

class Logger{
	
	static void info(String moduleNameString, String msgFormat, Object ... args ){
		String levelString = "[I] ";
		log(moduleNameString, levelString, msgFormat, args);
	}

	static void warn(String moduleName, String msgFormat, Object ... args ){
		String levelString = "[W] ";
		log(moduleName, levelString, msgFormat, args);
	}

	static void error(String moduleName, String msgFormat, Object ... args ){
		String levelString = "[E] ";
		log(moduleName, levelString, msgFormat, args);
	}

	static void debug(String msgFormat, Object ... args ){
		StringBuilder buff = new StringBuilder();
		buff.append(" [D] ");
		buff.append(String.format(msgFormat, args));
		System.out.println(buff.toString());
	}

	private static void log(String moduleName, String levelString, String msgFormat, Object ... args ){
		StringBuilder buff = new StringBuilder();
		String timeString = new SimpleDateFormat("yy/MM/dd HH:mm:ss ").format(new Date(System.currentTimeMillis()));
		buff.append(timeString);
		buff.append(levelString);
		if(moduleName!=null){
			buff.append(String.format("@%-16s ", moduleName));
		}
		buff.append(String.format(msgFormat, args));
		System.out.println(buff.toString());
	}
	
}
