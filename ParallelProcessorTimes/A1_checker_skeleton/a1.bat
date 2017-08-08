if exist a1.exe del a1.exe

fsc data.fs a1.fs
pause

rem Warning: not all arguments implemented
pause

.\a1.exe m-4-4.txt /* 0 0
pause

.\a1.exe m-4-4.txt /* 4 0
pause

.\a1.exe m-4-4.txt /* 2 1
pause

.\a1.exe m-4-4.txt /SEQ 2 1
pause

.\a1.exe m-4-4.txt /PAR-NAIVE 2 1
pause

.\a1.exe m-5-10-1.txt /* 0 0
pause

.\a1.exe m-5-10-1.txt /* 3 1
pause

rem Not implemented
pause

.\a1.exe m-4-4.txt /PAR-RANGE 2 1
pause

rem Medium
pause

.\a1.exe m-1000-300000-1.txt /* 0 0
pause

.\a1.exe m-1000-300000-lr.txt /* 0 0
pause

.\a1.exe m-1000-300000-rl.txt /* 0 0
pause

.\a1.exe m-1000-300000-quiz.txt /* 0 0
pause

rem Large
pause

.\a1.exe m-1000-1000000-1.txt /* 0 0
pause

.\a1.exe m-1000-1000000-lr.txt /* 0 0
pause

.\a1.exe m-1000-1000000-rl.txt /* 0 0
pause

.\a1.exe m-1000-1000000-quiz.txt /* 0 0
pause

rem Very large
pause

.\a1.exe m-10000-1000000-1.txt /* 0 0
pause

.\a1.exe m-1000-10000000-1.txt /* 0 0
pause

