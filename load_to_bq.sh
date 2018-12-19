#!/bin/bash


for level in class line #prod na
do
        for soar in 105 104 103 102 101
        do
                for ssn in basic ss fw
                do
                        echo Loading... 
                        bq load --autodetect --source_format=CSV syw-analytics-ff:apparel_jaccard_output.$level'_'$soar'_'$ssn gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/season/output/$level'_'$soar'_'$ssn'_'jaccard.csv/*
                done
        done
done
exit 0
