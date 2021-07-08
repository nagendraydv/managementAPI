import string, random
from codecs import encode, decode

def write_encrypted_file(path='.',filename='mysql.config'):
  with open(path + filename,'w') as f:
    for i in range(6):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,105)))))
    f.write('%r,\n' %(en[0]))
    for i in range(4):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,105)))))
    f.write('%r,\n' %(en[1]))
    for i in range(1):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,105)))))
    f.write('%r,\n' %(en[2]))
    for i in range(5):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,95)))))
    f.write('%r,\n' %(en[3]))
    for i in range(9):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,125)))))

# Config file for recoengine

path = "."
#h,u,p,d = ["172.17.3.79", "mintwalk_cron", "Mwdb@*716", "mint_loan"] #162
h,u,p,d = ["172.17.3.194", "sandesh", "Sandesh#2018", "mint_loan"]#"prodcron", "ProdCron#2018", "mint_loan"] #172.17.3.79
#en = [h.encode('uu').encode('base64'),u.encode('bz2'),p.encode('bz2').encode('base64'),d.encode('base64')]
en = [encode(h.encode(), 'base-64'), encode(u.encode(),'uu'), encode(encode(p.encode(),'uu'), 'base-64'), encode(encode(d.encode(), 'base64'), 'uu')]
#write_encrypted_file(path=path,filename='mysql-super.config')

path = "."
#h,u,p,d = ["172.17.3.192", "prodcron", "kkA2341uywe!#^", "mint_walk_admin"]
h,u,p,d = ["164.52.193.97", "prodcron", "ProdCron#2018", "mint_walk_admin"] #172.17.3.192 #172.17.3.106
#en = [h.encode('uu').encode('base64'),u.encode('bz2'),p.encode('bz2').encode('base64'),d.encode('base64')]
en = [encode(h.encode(), 'base-64'), encode(u.encode(),'uu'), encode(encode(p.encode(),'uu'), 'base-64'), encode(encode(d.encode(), 'base64'), 'uu')]
#write_encrypted_file(path=path,filename='mysql.config')

path = "."
#h,u,p,d = ["13.126.28.53", "mintwkdevphp", "MintDB#2017", "mint_walk_admin"]
h,u,p,d = ["101.53.155.108", "sandesh", "Sandesh#2018", "mint_walk_admin"]
#en = [h.encode('uu').encode('base64'),u.encode('bz2'),p.encode('bz2').encode('base64'),d.encode('base64')]
en = [encode(h.encode(), 'base-64'), encode(u.encode(),'uu'), encode(encode(p.encode(),'uu'), 'base-64'), encode(encode(d.encode(), 'base64'), 'uu')]
write_encrypted_file(path=path,filename='mysql-dev.config')

path = "."
#h,u,p,d = ["172.17.12.60", "cronuser", "Mintwk@2516", "accordmfdata"]
h,u,p,d = ["172.17.12.19", "prodcron", "Prodcron#2018", "accordmfdata"] #172.17.12.60
#en = [h.encode('uu').encode('base64'),u.encode('bz2'),p.encode('bz2').encode('base64'),d.encode('base64')]
en = [encode(h.encode(), 'base-64'), encode(u.encode(),'uu'), encode(encode(p.encode(),'uu'), 'base-64'), encode(encode(d.encode(), 'base64'), 'uu')]
#write_encrypted_file(path=path,filename='mysql-accord.config')

path = "."
h,u,p,d = ["172.17.12.207", "prodcron", "Prodcron#2018", "mint_loan"]
#en = [h.encode('uu').encode('base64'),u.encode('bz2'),p.encode('bz2').encode('base64'),d.encode('base64')]
en = [encode(h.encode(), 'base-64'), encode(u.encode(),'uu'), encode(encode(p.encode(),'uu'), 'base-64'), encode(encode(d.encode(), 'base64'), 'uu')]
#write_encrypted_file(filename='mysql-slave.config')
