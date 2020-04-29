def reducer(x):
    x1 = list(x[1])
    m1, m2 = [], []
    for i in x1:
        if(list(i)[0] == 0):
            m1.append(list(i))
        else:
            m2.append(list(i))
     
    m1 = sorted(m1, key=lambda x: x[1] )
    m2 = sorted(m2, key=lambda x: x[1] )
    maxIndex = max(m1[-1][1], m2[-1][1])
    for i in range(maxIndex):
        if(m1[i][1] != i):
            m1.insert(i, None)
        if(m2[i][1] != i):
            m2.insert(i, None)
    
    val = 0
    
    for i in range(maxIndex+1):
        if(m1[i] == None or m2[i] == None ):
            val  = val
        elif(m1[i][1] == i and m2[i][1] == i):
            val += (m1[i][2]*m2[i][2] )

    return (x[0], val)	
