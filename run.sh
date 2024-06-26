# python -m webs.run failure-injection \
#         --executor parsl \
#         --true-workflow mapreduce \
#         --failure-rate 1 \
#         --failure-type dependency


python -m webs.run failure-injection \
        --executor parsl \
        --true-workflow moldesign \
        --failure-rate 0 \
        --failure-type dependency
