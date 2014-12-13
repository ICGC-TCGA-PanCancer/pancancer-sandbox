#!/bin/bash

if [[ -z $1 ]]; then
	echo "You must specifiy the workpath of the oozie job."
	exit 1
fi

cd $1/generated-scripts
cat vcfUpload_319.sh | sed 's/icgc_pancancer_vcf_test --skip-upload/tcga_pancancer_vcf_test/g' | sed 's/--key \/glusterfs\/netapp\/homes1\/BOCONNOR\/.ssh\/gnos-osdc-tcga-key.pem/--key \/glusterfs\/netapp\/homes1\/BOCONNOR\/.ssh\/bionimbus_tcga_gnos_20141125.pem/g' > upload_patch.sh
mv vcfUpload_319.sh vcfUpload_319.shoff
mv upload_patch.sh vcfUpload_319.sh
chmod +x vcfUpload_319.sh
