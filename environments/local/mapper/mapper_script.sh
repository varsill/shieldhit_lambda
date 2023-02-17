cp /home/input/* /home
cp /home/binaries/* /home
cd home
./shieldhit -n $1 -N $2
cp *.bdo /home/results