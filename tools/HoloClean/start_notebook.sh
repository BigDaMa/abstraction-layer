#source activate py27Env
#source /etc/profile
#source activate py27Env


# Set & move to home directory
source set_env.sh
cd "$HOLOCLEANHOME"

# Launch jupyter notebook!
echo "Launching Jupyter Notebook..."
#jupyter notebook

python2 ./run1.py
