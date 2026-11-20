import socket, sys, time
def cmd(port, line):
    s = socket.create_connection(("localhost", port), timeout=3)
    s.sendall((line+"\n").encode())
    try: r = s.recv(256).decode().strip()
    except socket.timeout: r = "(sin respuesta)"
    s.close(); return r
B = {0:9000, 1:9001, 2:9002}
for r,p in B.items(): print(f"PING b{r}  ->", cmd(p,"PING"))
for r,p in B.items(): print(f"STATE b{r} ->", cmd(p,"STATE"))
print("b0 PAUSE 0 (local)   ->", cmd(9000,"PAUSE 0"))
print("b0 PING (pausado)    ->", cmd(9000,"PING"))
print("b0 RESUME 0          ->", cmd(9000,"RESUME 0"))
print("b0 PAUSE 2 (por MPI) ->", cmd(9000,"PAUSE 2")); time.sleep(0.3)
print("b2 STATE             ->", cmd(9002,"STATE"))
print("b0 RESUME 2 (MPI)    ->", cmd(9000,"RESUME 2")); time.sleep(0.3)
print("b2 STATE             ->", cmd(9002,"STATE"))
print("comando raro         ->", cmd(9001,"FOO"))
for r,p in B.items(): print(f"SHUTDOWN b{r} ->", cmd(p,"SHUTDOWN"))
