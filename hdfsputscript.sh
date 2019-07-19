USAGE="USAGE:$(basename $0) <local_path> <hdfs_path>"

if [ $# -ne 2 ]
then
	echo $USAGE
	exit
fi

local_path=$1
hdfs_path=$2

get_file_name=`echo "${local_path##*/}"`

# check if the file is already existing at the hdfs location
chk_path="$hdfs_path/$get_file_name"
ret=`hadoop fs -test -z ${chk_path};echo $?`

if [ $ret = 255 ]
then
	res=`hadoop fs -put ${local_path} ${hdfs_path};echo $?`
	echo "File CampaignData_full.csv has been copied to HDFS location $hdfs_path"
else
	echo "File already exists"
fi
