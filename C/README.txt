To run the code we need some softwares and tehnologies.

If you do not have Linux, I advise you to use a virtual machine with linux to run all the code. 
You can use virtual box. It is really easy to use. find an small tutorial below:
https://www.nakivo.com/blog/install-ubuntu-on-virtualbox-virtual-machine/

- Install IntellIJ IDE. You can find it on Jbrains link below:
https://www.jetbrains.com/fr-fr/idea/download/#section=windows

- Install git. You can find an easy tutorial to install git with the link below:
https://git-scm.com/downloads

- Install Apache Flink. You can find a tutorial with the link below:
https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/python/installation/

- Install Scala plugin for IntellIJ following step shared by the link below
https://www.jetbrains.com/help/idea/managing-plugins.html

If you already have the full project on your desktop, skip this step: Open a command prompt where you want the project to be cloned and clone the below link with command:
git clone https://github.com/CANEVETGASPARD/HW1_CANEVET.git

Then open project C with intellIJ and update build properties of the project expecially your java and scala version. 
If you want to know your scala and java version. Open a cmd and write:
sclala -version
java -version

Then load the input file with the link given in Homework paper and put it in a file named input

Finally, we have to set up run-configuration for the task I link to ShortestPath.scala main function. To do so, you have to use the configuration shared in the report (fig 18).

Our project is now set up we just have to run the code.