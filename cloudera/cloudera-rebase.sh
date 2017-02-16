#!/bin/bash

DOWNSTREAM_BRANCH=
UPSTREAM_BRANCH=
PROJECT_NAME=
IGNORE_COMMITS_FILE=
OUTPUT_FILE=

CSV_DELIMITER="|"

display_help() {
	echo "Usage: `basename $0` --downstream BRANCH --upstream BRANCH --project NAME --output FILE [OPTIONS]"
	echo 	
	echo "  --downstream            Branch name for downstream"
	echo "  --upstream              Branch name for upstream"
	echo "  --project               Project name (i.e. PARQUET, HIVE, AVRO)"
	echo "  --output                Output file of commits"
	echo
	echo "OPTIONS"
	echo "  --ignore-commits-file   File that contains commits to ignore"
}

while [ $# -ne 0 ]; do
	optname="$1"

	case "$optname" in
		--downstream)
			DOWNSTREAM_BRANCH="$2"
			shift 2
		;;
		--upstream)
			UPSTREAM_BRANCH="$2"
			shift 2
		;;
		--project)
			PROJECT_NAME="$2"
			shift 2
		;;
		--ignore-commits-file)
			IGNORE_COMMITS_FILE="$2"
			shift 2
		;;
		--output)
			OUTPUT_FILE="$2"
			shift 2
		;;
		*)
			shift
		;;
	esac
done

([ -z $DOWNSTREAM_BRANCH ] || [ -z $UPSTREAM_BRANCH ] || [ -z $PROJECT_NAME ] || [ -z $OUTPUT_FILE ])  && display_help && exit 1

echo "Collecting list of commits from '$DOWNSTREAM_BRANCH' that need to be applied on '$UPSTREAM_BRANCH' ..."

CHERRY_FILE=$(mktemp)
trap "rm $CHERRY_FILE" SIGINT
git cherry -v $UPSTREAM_BRANCH $DOWNSTREAM_BRANCH | grep '^\+' | sed 's/^+ //g' > $CHERRY_FILE
echo `cat $CHERRY_FILE | wc -l` "commits found."

echo

get_commit_id() {
	echo $@ | cut -d' ' -f1
}

get_commit_message() {
	echo $@ | cut -d' ' -f2-
}

add_csv_row() { 
	local IFS="$CSV_DELIMITER"
	echo "$*" >> $OUTPUT_FILE
}

PROJECT="1"
CLOUDERA_BUILD="2"
FIXUP="3"
OTHER="4"

get_commit_type() {
	if [[ "$@" =~ ^${PROJECT_NAME}-[0-9]+ ]]; then
		echo $PROJECT
	elif [[ "$@" =~ CLOUDERA-BUILD|CLOUDERA_BUILD ]]; then		
		echo $CLOUDERA_BUILD
	elif [[ "$@" =~ FIXUP ]]; then
		echo $FIXUP
	else
		echo $OTHER
	fi
}

test_cherry_pick_commit() {	
	if ! git cherry-pick $1 >/dev/null 2>&1; then
		git cherry-pick --abort
		return 1
	fi

	git reset --hard HEAD^1 >/dev/null
	return 0
}

print_cherry_pick_success() {
	add_csv_row "[ CHERRYPICK WORKS ]" "$1" "$2"
}

print_found_reference() {
	add_csv_row "[ FOUND REFERENCE ]" "$1" "$2"	
}

print_cherry_pick_fail() {
	add_csv_row "[ CHERRYPICK FAILS ]" "$1" "$2"	
}

print_unknown_commit() {
	add_csv_row "[ UNKNOWN COMMIT ]" "$1" "$2"	
}

print_cherry_pick_found() {
	add_csv_row "[ ALREADY IMPLEMENTED ]" "$1" "$2"	
}

print_ignore_commit() {
	add_csv_row "[ COMMIT NOT NEEDED ]" "$1" "$2"	
}

commit_is_found() {		
	commits=`git log --fixed-strings --grep="$1" --pretty=format:'%H %s' --abbrev-commit`
	if [ -n "$commits" ]; then		
		# found
		return 0
	fi

	return 1
}

get_jira_commit_message() {
	echo "$@" | sed 's/[\ :]*Backport[\ :.-]*//g' | sed 's/[\ :]*PROPOSED[\ :.-]*//g' |  sed 's/[\ :]*CDH-[0-9]\+[\ :.-]*//g'
}

is_commit_reviewed() {
	if [ -n "$COMMITS_REVIEWED" ]; then
		grep "$1" $COMMITS_REVIEWED >/dev/null 2>&1
	else
		return 1
	fi
}

TOTAL_COMMITS=`cat $CHERRY_FILE | wc -l`
counter=0

# reset output file
rm -f $OUTPUT_FILE

# Need to switch to upstream branch to validate cherry-picks
git checkout $UPSTREAM_BRANCH || exit 1

cat $CHERRY_FILE | while read line; do
	counter=$(( $counter + 1 ))
	echo "Verifying commit $counter out of $TOTAL_COMMITS ..."

	commit_id=`get_commit_id $line`
	commit_msg=`get_commit_message $line`

	jira_commit_msg=`get_jira_commit_message $commit_msg`	
	commit_type=`get_commit_type $jira_commit_msg`	

	if is_commit_reviewed "$commit_id"; then
		continue
	fi

	case "$commit_type" in
		$PROJECT)				
			jira_id=`echo $jira_commit_msg | grep -oh "${PROJECT_NAME}-[0-9]\+"`			
			if ! commit_is_found "$jira_commit_msg"; then	
				if commit_is_found "$jira_id"; then
					print_found_reference "$commit_id" "$commit_msg"	
				elif test_cherry_pick_commit "$commit_id"; then
					print_cherry_pick_success "$commit_id" "$commit_msg"
				else
					print_cherry_pick_fail "$commit_id" "$commit_msg"
				fi
			else
				print_cherry_pick_found "$commit_id" "$commit_msg"				
			fi			
		;;
		$CLOUDERA_BUILD)			
			if ! commit_is_found "$commit_msg"; then							
				if test_cherry_pick_commit "$commit_id"; then
					print_cherry_pick_success "$commit_id" "$commit_msg"
				else
					print_cherry_pick_fail "$commit_id" "$commit_msg"
				fi				
			else
				print_cherry_pick_found "$commit_id" "$commit_msg"
			fi			
		;;
		$FIXUP)
			print_ignore_commit "$commit_id" "$commit_msg"
		;;
		*)
			print_unknown_commit "$commit_id" "$commit_msg"
		;;
	esac
done