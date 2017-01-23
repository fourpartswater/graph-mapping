#!/bin/bash



SOURCE_LOCATION=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
. ${SOURCE_LOCATION}/level-config.sh



function parseArguments {
        PREFIX=$1
        shift
        SUFFIX=$1
        shift

        DATASET=
        TOP_LEVEL=3
        NEXT_LEVELS=2
        LEVEL_METHOD=hard

        while [ "$1" != "" ]; do
                case $1 in
                        -d | --dataset )
                                shift
                                DATASET=$1
                                ;;
                        -s | --suffix )
                                shift
                                SUFFIX=$1
                                ;;
                        -p | --prefix )
                                shift
                                PREFEX=$1
                                ;;
                        -l | --levels )
                                shift
                                case $1 in
                                        h | hard )
                                                LEVEL_METHOD=hard
                                                ;;
                                        s | stats )
                                                LEVEL_METHOD=stats
                                                ;;
                                        u | user-specified )
                                                LEVEL_METHOD=user
                                                shift
                                                USER_LEVELS=$1
                                                ;;
                                esac
                                ;;
                        -1 | -t | --top )
                                shift
                                TOP_LEVEL=$1
                                ;;
                        -n | -b | --next | --bottom )
                                shift
                                NEXT_LEVELS=$1
                                ;;
                        -m | --max-executors )
                                shift
                                export MAX_EXECUTORS=$1
                                ;;
                        -base )
                                shift
                                BASE_LOCATION=$1
                                echo Set base directory to ${BASE_LOCATION}
                                ;;
                        -debug )
                                export DEBUG=true
                                ;;
                esac
                shift
        done

        if [ "${DATASET}" == "" ]; then
                echo No dataset specified
                exit
        fi


        case $LEVEL_METHOD in
                hard )
                        LEVELS=($(hardCodedLevels ${DATASET} ${TOP_LEVEL} ${NEXT_LEVELS}))
                        ;;
                stats )
                        LEVELS=($(levelsFromStats ${DATASET}))
                        ;;
                user )
                        LEVELS=(${USER_LEVELS[*]})
                        ;;
                * )
                        echo Invalid level determination method specified
                        exit
                        ;;
        esac

        DATATABLE=${PREFIX}${DATASET}${SUFFIX}

        export DATASET
        export DATATABLE
        export LEVELS
}
