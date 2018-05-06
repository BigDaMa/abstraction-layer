#set the prepration for the Holoclean


#source activate py27
#source /etc/profile
#source activate py27

# Set & move to home directory
source set_env.sh
cd "$HOLOCLEANHOME"

# Launch jupyter notebook!
echo "Launching Jupyter Notebook..."
jupyter notebook

#python2 /home/milad/Desktop/HoloClean/tutorials/Holoclean_Hospital.py
