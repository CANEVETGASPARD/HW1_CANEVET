To run the code we need some softwares and tehnologies.

If you do not have Linux, I advise you to use a virtual machine with linux to run all the code. 
You can use virtual box. It is really easy to use. find an small tutorial below:
https://www.nakivo.com/blog/install-ubuntu-on-virtualbox-virtual-machine/

- Install IntellIJ IDE. You can find it on Jbrains link below:
https://www.jetbrains.com/fr-fr/idea/download/#section=windows

- Install git. You can find an easy tutorial to install git with the link below:
https://git-scm.com/downloads

- Install Scala plugin for IntellIJ following steps shared by the link below
https://www.jetbrains.com/help/idea/managing-plugins.html

-Install Apache spark. You can find a tutorial for Linux with the link below:
https://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm

If you already have the full project on your desktop, skip this step: Open a command prompt where you want the project to be cloned and clone the below link with command:
git clone https://github.com/CANEVETGASPARD/HW1_CANEVET.git

Then open project B with intellIJ and update build properties of the project expecially your java and scala version. 
If you want to know your scala and java version. Open a cmd and write:
sclala -version
java -version

Then load the input files with the link given in Homework paper and put them in a file named input

Finally, we have to set up run-configuration. One for the task I and one the task III repectively link with the CustomersOrders.scala main function and ShortestPath.scala main function. To do so use configurations shared in the report (fig 9, fig 13). Do not forget to use -Xss512mb flag to prevent stackoverflow error in the Task III.

Our project is now set up we just have to run the code.