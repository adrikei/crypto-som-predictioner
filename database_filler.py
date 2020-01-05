from sys import argv
import json

table = argv[1]

head_query = 'INSERT INTO %s (id, price, quantity, ts, buyer_maker, best) VALUES ' % (table)
insert_query = ' (%d, %f, %f, to_timestamp(%d/1000), %s, %s)'

history_file = './history_%s' % (table)

file = open(history_file, 'r')

count = 0
printline = head_query
qline = []

for line in file:
    lineJSON = json.loads(line)
    id = int(lineJSON['a'])
    price = float(lineJSON['p'])
    quantity = float(lineJSON['q'])
    ts = int(lineJSON['T'])
    b_m = str(lineJSON['m']).upper()
    best = str(lineJSON['M']).upper()
    q = insert_query % (id, price, quantity, ts, b_m, best)
    qline.append(q)
    count = count + 1
    if(count == 1000) :
        printline = head_query + ','.join(qline) + ';'
        print(printline)
        count = 0
        qline = []

printline = head_query + ','.join(qline) + ';'
print(printline)