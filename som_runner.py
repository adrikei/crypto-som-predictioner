    import psycopg2
    import pandas as pd
    from minisom import MiniSom
    import numpy as np
    from pylab import bone, pcolor, colorbar, plot, show
    
    succeeded_connecting = False
    try:
        conn = psycopg2.connect("dbname='adriano' user='adriano' host='localhost' password='adriano'")
        succeeded_connecting = True
    except:
        print("I am unable to connect to the database")
    
    if succeeded_connecting:
        cur = conn.cursor()
        cur.execute("""select ts,
                    wavg_lr,
                    ema13, ema21, ema55, rsi2, rsi14, 
                    macd_macd, macd_signal macd_hist, ultosc, willr, obv,
                    
                    100*(1-(select f.wavg/c.wavg from bu_sa_t5 f where f.ts = (c.ts + interval '30 seconds')))
                    from bu_sa_t10 c where c.ts >= '2018-05-01' and c.ts < '2018-06-01' order by ts""")
        rows = cur.fetchall()
        df = pd.DataFrame(rows)
        del rows
        df.drop(columns=[0], inplace=True)
    
        result = df.iloc[:, df.shape[1]-1:]
        df = df.iloc[:, :df.shape[1]-1]
        
        length = df.shape[0]
        width = df.shape[1]
        
        # calculated using the rule-of-thumb in the minisom documentation
        dim = int(np.ceil((1*length**(1/2.))**(1/2.)))
#        dim = int(np.ceil((1*length**(1/2.))**(1/2.)))    
        # Experimental
        dimx = int(np.ceil((1*length**(1/2.))**(.618)))
        dimy = int(np.ceil((1*length**(1/2.))**(1-.618)))
        print('dimensions is %d or (%d %d)' % (dim, dimx, dimy))
        
        som = MiniSom(x = dim, y = dim, input_len=width, sigma=1.0, learning_rate=0.5)
#        som = MiniSom(x = dimx, y = dimy, input_len=width, sigma=1.0, learning_rate=0.5)
        
#        som.random_weights_init(df.values)
        som.train_random(df.values, 10000)
        #del df
        
        ######
        bone()
        pcolor(som.distance_map().T)
        colorbar()
        ######
        target = 0.5 # 0.5 => 0.5%
        
        y = np.where(result[12] > (target), 1, np.where(result[12] < (-1)*target, -1, 0))
        
        for i, x in df[y[df.index] != 0].iterrows():
            #print(x.values)
            w = som.winner(x.values)
            if y[i] == 1:
                plot(w[0] + .5,
                     w[1] + .5,
                     'x',
                     markeredgecolor = 'g',
                     markerfacecolor = 'None',
                     markersize = 10,
                     markeredgewidth = 2)
            elif y[i] == -1:
                plot(w[0] + .5,
                     w[1] + .5,
                     's',
                     markeredgecolor = 'r',
                     markerfacecolor = 'None',
                     markersize = 10,
                     markeredgewidth = 2)
        
        nz = np.nonzero(y)[0]
        
        for i in range(nz[0]-50, nz[0]+1):
            x = df.iloc[i]
            w = som.winner(x.values)
            plot(w[0] + .5,
                 w[1] + .5,
                 '.',
                 markeredgecolor = 'y',
                 markerfacecolor = 'None',
                 markersize = 5,
                 markeredgewidth = 1)
    
    show()
    del df
    
    cur.close()
    conn.close()