import subprocess
import pip
import git
import sys



install_input={
    "Tool":{
        "tool_name":"Nadeef"
    }

}


if (install_input["Tool"]["tool_name"]=="Nadeef"):

    #x=subprocess.Popen("git clone https://github.com/jkbr/httpie.git\ngit clone https://github.com/daqcri/NADEEF.git\ncd NADEEF\nant all\n./nadeef.sh\n",stdout=subprocess.PIPE,stdin=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
    x=subprocess.Popen("git clone https://github.com/jkbr/httpie.git\ngit clone https://github.com/daqcri/NADEEF.git\n",stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    x.communicate(" ")
    print "Nadeef is installed"
    # out,erro=x.communicate("load ../nadeef/nadeef_configuration.json\ndetect\nexit\n")
    # print (out)
    # print (erro)




if (install_input["Tool"]["tool_name"]=="Dboost"):


    x=subprocess.Popen("git clone https://github.com/jkbr/httpie.git\ngit clone https://github.com/cpitclaudel/dBoost.git\n",stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    x.communicate(" ")
    print "Dboost is installed"