# IP="127.1"
# PORT=8081
python -m  Producers.p1 > log_p1&
python -m  Producers.p2 > log_p2& 
python -m  Producers.p3 > log_p3& 
python -m  Producers.p4 > log_p4& 
python -m  Producers.p5 > log_p5& 
python -m  Consumers.c1 > log_c1& 
python -m  Consumers.c2 > log_c2& 
python -m  Consumers.c3 > log_c3&