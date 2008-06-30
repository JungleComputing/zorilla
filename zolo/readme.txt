-----------
ZOLO README
-----------
Zolo is an extension for the Zorilla Starter application. Zolo starts and
stops Zorilla using a set of utilization strategies. Zorilla Starter is a
simple application written in Java to start, download and run Zorilla.
Zolo allows the user to choose one or more machine utilization strategies
from a predefined set of available strategies. Together with the Zolo
configuration, these strategies are used to determine when Zorilla is
allowed to run.

Zolo is written by Wouter Leenards - wleenar@cs.vu.nl



Install Zolo
------------
Unzip the Zolo zipfile to a directory of choice.


Running Zolo
------------
Zolo can be started manually and installed as Windows service or Linux
daemon.

WINDOWS:
Start Zolo manually:       zolo-windows-console.bat [options]
Install Zolo as service:   zolo-windows-service-install.bat
Start Zolo service:        zolo-windows-service-start.bat
Stop Zolo service:         zolo-windows-service-start.bat
Uninstall Zolo service:    zolo-windows-service-uninstall.bat
When Zolo is running as service, go to Start -> Run and type services.msc
to see all services currently installed (and running).

UNIX:
Start Zolo manually:       ./zolo-linux [options]
Manage Zolo Daemon:        ./zolo-linux-daemon { console | start |
                                  stop | restart | status | dump }


Configure Zolo
----------------
Edit the default configuration file (zolo-config.xml) while Zolo is not
running.


Implementing new strategies
---------------------------
It is very simple to add extra strategies to Zolo. Create a new java
source file and name it YourStrategyNameStrategy.java.
A few rules for your strategy:
- Package name: zolo.strategy
- It must extend Strategy
- It must implement StrategyInterface


Compile Zolo
------------
When you made some changes to Zolo and want to recompile, use ant:
ant <enter>
