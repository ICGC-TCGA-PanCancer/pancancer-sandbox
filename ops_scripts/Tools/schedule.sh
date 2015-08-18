#!/bin/bash

function pemPush() {
x=${1}
        scp -i ~/.ssh/niall-oicr-1.pem ~/.ssh/gnos.pem ubuntu@$x:gnostest.pem
        ssh -i ~/.ssh/niall-oicr-1.pem ubuntu@$1 "sudo cp *.pem /mnt/home/seqware && sudo chown seqware:seqware /mnt/home/seqware/*.pem"

}

function schedule() {

for x in `cat slaves`; do

        for y in `ls *.ini`; do

                echo $x will run ini file $y
                ssh -i ~/.ssh/niall-oicr-1.pem ubuntu@$x "sudo cp *.ini /mnt/home/seqware/ini && sudo chown seqware:seqware /mnt/home/seqware/ini/*.ini" 
                scp -i ~/.ssh/niall-oicr-1.pem $y ubuntu@$x:
                mkdir $x 2> /dev/null
                mv $y $x
                break

        done

done

}

function unschedule() {

for x in `cat slaves`; do
        ssh -i ~/.ssh/niall-oicr-1.pem ubuntu@$x "sudo rm /mnt/home/seqware/*.ini"
        mv $x/*.ini .

done

}

function onehost() {

x=${1}

        for y in `ls *.ini`; do

                echo $x will run ini file $y
                ssh -i ~/.ssh/niall-oicr-1.pem ubuntu@$x "sudo cp *.ini /mnt/home/seqware/ini && sudo chown seqware:seqware /mnt/home/seqware/ini/*.ini"
                scp -i ~/.ssh/niall-oicr-1.pem $y ubuntu@$x:
                mkdir $x 2> /dev/null
                mv $y $x
                break

        done

}

oneHost $1
