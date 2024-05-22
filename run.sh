python -m webs.run failure-injection \
        --executor parsl \
        --true-workflow mapreduce \
        --failure-rate 0 \
        # --mode=random \
        # --map-task-count=2
