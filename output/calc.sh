outfile="../lantency.txt"
for f in `ls .`
do
	echo calc $f >> $outfile
#	echo read: >> $outfile
#	awk -F , 'BEGIN{c=0;f=0;} {if($1=="r"){c+=1;f+=$4;}} END{print f/c;}' $f >> $outfile
#	echo write: >> $outfile
#	awk -F , 'BEGIN{c=0;f=0;} {if($1=="w"){c+=1;f+=$4;}} END{print f/c;}' $f >> $outfile
	echo total: >> $outfile
	awk -F , 'BEGIN{c=0;f=0;} {{c+=1;f+=$4;}} END{print f/c;}' $f >> $outfile
	echo >> $outfile
done
