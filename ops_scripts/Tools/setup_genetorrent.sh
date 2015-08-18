#/bin/bash

# I run these commands all the time, this is easier

version=$(lsb_release -a | grep Release | awk '{print $2}')

echo "Installing genetorrent ..."
echo ""

wget https://cghub.ucsc.edu/software/downloads/GeneTorrent/3.8.7/genetorrent-common_3.8.7-ubuntu2.207-${version}_amd64.deb
wget https://cghub.ucsc.edu/software/downloads/GeneTorrent/3.8.7/genetorrent-download_3.8.7-ubuntu2.207-${version}_amd64.deb
wget https://cghub.ucsc.edu/software/downloads/GeneTorrent/3.8.7/genetorrent-upload_3.8.7-ubuntu2.207-${version}_amd64.deb

sudo dpkg -i *.deb 2> /dev/null 1> /dev/null

sudo apt-get update > /dev/null
sudo apt-get -fy install > /dev/null

rm *.deb

echo ""
echo "All done."
echo ""

