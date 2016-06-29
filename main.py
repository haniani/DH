import pymysql as db
import csv, os, sys
import pylab
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats
from nltk.tokenize.simple import SpaceTokenizer
from nltk.stem.porter import PorterStemmer
import gensim
from gensim import corpora, models, similarities
from stop_words import get_stop_words


class sql_worker:

    def __init__(self, USER, PASSWD, HOST, CHARSET, DATABASE, COLLATE):
        self.user = USER
        self.passwd = PASSWD
        self.host = HOST
        self.charset = CHARSET
        self.database = DATABASE
        self.collate = COLLATE

    def connect(self): #подключаемся 
        try:
            self.connection = db.connect(host=self.host, user=self.user, passwd=self.passwd, charset=self.charset)
        except:
            return False
        else:
            return True

    def create(self): #создаем базу
        try:
            with self.connection.cursor() as cursor:
                self.creation = cursor.execute("create database `%s` DEFAULT CHARSET=`%s` COLLATE=%s;" % (self.database, self.charset, self.collate))
        except:
            return False
        else:
            return True

    def use(self, db): #выбираем базу
        try:
            with self.connection.cursor() as cursor:
                self.use = cursor.execute("use `%s`;" % db)
        except:
            return False
        else:
            return True

    def create_table(self, tablename, fields): #создаем таблицу
        self.tbl = tablename
        self.fields = fields

        try:
            with self.connection.cursor() as cursor:
                self.use = cursor.execute("create table %s (%s) DEFAULT CHARSET=%s COLLATE=%s;" % (self.tbl, self.fields, self.charset, self.collate))
        except:
            return False
        else:
            return True

    def query(self, query):  #запрос
        try:
            with self.connection.cursor() as cursor:
                self.querys = cursor.execute(query)
                self.connection.commit()
        except:
            return False
        else:
            return self.querys

    def select(self, query): #запрос select
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
        except Exception as e:
            return e
        else:
            return cursor.fetchall()

    def close(self): #закрываем соединение
        try:
            with self.connection.cursor() as cursor:
                cursor.close()
        except:
            return False
        else:
            return True


def main():
    USER = "root"
    PASSWD = "089567"
    HOST = "localhost"
    CHARSET = "utf8mb4"
    DATABASE = "music"
    COLLATE = "utf8mb4_unicode_ci"
    #PORT = "3306"
    PATH = './'
    PATH_U = './users'

    x = sql_worker(USER, PASSWD, HOST, CHARSET, DATABASE, COLLATE)
    print("Соединение", x.connect())
    print("Создание", x.create())
    print("Использование", x.use(DATABASE))
    print("Ожидайте, обработка базы")

    dlalbom = []
    dlpesnya = []
    textsong = []
    artist = (x.select("select id,name from wc_lyricsnet_artists limit 1000"))
    for elem in artist:
        album = (x.select("select `id`,`name` from `wc_lyricsnet_albums` where `artist_id` = '%s' and `year` = 1995;" % (elem[0])))
        for each in album:
            song = (x.select("select `title` from `wc_lyricsnet_songs` where `artist_id` = '%s' and `album_id` = '%s';" % (elem[0], each[0])))
            for ef in song:
                ef = list(ef)
                ef = ''.join(map(str, ef))
                textsong.append(ef)
                #csvwww.write(str(elem[1]) + "\t" + str(each[1]) + "\t" + str(ef) + "\n") #создание словаря
                #print(elem[1])
                #print(each[1])
                #print(ef)
                dlinaalbom = len(str(each[1]))
                dlinasong = len(str(ef))
                dlalbom.append(dlinaalbom)
                dlpesnya.append(dlinasong)
         

    xx = np.array(dlalbom)
    yy = np.array(dlpesnya)
    pylab.plot(yy, xx, 'r')
    pylab.xlabel('Количество символов в названии песни')
    pylab.ylabel('Количество символов в названии альбома')
    pylab.title('Соотношение длины названий (в символах)')
    pylab.show()


    correl = np.corrcoef(yy, xx)
    print("Корреляция длины названий песен и альбомов")
    print("Для продолжения закройте диаграмму")
    print("###")
    print(correl)
    plt.scatter(yy, xx)
    plt.show()


    print("Корреляция Пирсона", scipy.stats.pearsonr(yy, xx))
    print("\n")       
                								#LDA
    texts = []
    tokentoken = SpaceTokenizer()     #токенизация
    en_stop = get_stop_words('en')   #стоп-слова
    p_stemmer = PorterStemmer()     #стемминг      
    

    for i in textsong:
        raw = i.lower()
        tokens = tokentoken.tokenize(raw)

        stopped_tokens = [i for i in tokens if not i in en_stop]
        stemmed_tokens = [p_stemmer.stem(i) for i in stopped_tokens]
        texts.append(stemmed_tokens)

    dic = corpora.Dictionary(texts)
    corps = [dic.doc2bow(text) for text in texts]

    ldamodel = gensim.models.ldamodel.LdaModel(corps, num_topics = 7, id2word = dic, passes = 20)
    try:
        plt.style.use('ggplot')
    except:
        pass

    print("Результат работы LDA:")
    print("###")
    print(ldamodel.print_topics(num_topics = 7, num_words = 3))
    print("\n")


    allwords = []
    for elements in texts:
        for allell in elements:
            allwords.append(allell)

    stoplist = set('for a of the and to in'.split())              #LSI 
    texts = [[word for word in document.lower().split() if word not in stoplist]
             for document in allwords]
    alltokens = sum(texts, [])
    tokens1 = set(word for word in set(alltokens) if alltokens.count(word) == 1)
    texts = [[word for word in text if word not in tokens1] for text in texts]
    corp = [dic.doc2bow(text) for text in texts]
    lsi = models.lsimodel.LsiModel(corpus = corp, id2word = dic, num_topics = 3)
    print("Результат работы LSI:")
    print("###")
    print(lsi.print_topics(3))

    
    slovnik = {}

    with open("csvdict1.csv", 'r') as f:
        reader = csv.reader(f, delimiter = "\t")
        for row in reader:
            kluch1 = row[2].split(" ")
            for elemk in kluch1:
                kluch = elemk
                kluch = str(kluch).lower()
                if kluch in slovnik:
                    znach = slovnik[kluch]
                    slovnik[kluch] = znach + 1
                else:
                    slovnik[kluch] = 1

    stopslova = "a, about, above, after, again, against, all, am, an, and, any, are, aren't, as, at, be, because, been, before, being, below, between, both, but, by, can't, cannot, could, couldn't, did, didn't, do, does, doesn't, doing, don't, down, during, each, few, for, from, further, had, hadn't, has, hasn't, have, haven't, having, he, he'd, he'll, he's, her, here, here's, hers, herself, him, himself, his, how, how's, i, i'd, i'll, i'm, i've, if, in, into, is, isn't, it, it's, its, itself, let's, me, more, most, mustn't, my, myself, no, nor, not, of, off, on, once, only, or, other, ought, our, ours ourselves, out, over, own, same, shan't, she, she'd, she'll, she's, should, shouldn't, so, some, such, than, that, that's, the, their, theirs, them, themselves, then, there, there's, these, they, they'd, they'll, they're, they've, this, those, through, to, too, under, until, up, very, was, wasn't, we, we'd, we'll, we're, we've, were, weren't, what, what's, when, when's, where, where's, which, while, who, who's, whom, why, why's, with, won't, would, wouldn't, you, you'd, you'll, you're, you've, your, yours, yourself, yourselves"
    sortklucha = sorted(slovnik, key = lambda x: int(slovnik[x]), reverse = True)
    zapis = open("slovnik.csv",'w')
    zapis2 = open("slovnikstopwords.csv", "w")

    zapis.write("Word" + "\t" + "Frequency" + "\n")
    zapis2.write("Word" + "\t" + "Frequency" + "\n")
    try:
        for kluch in sortklucha:
                jjj = str("{0}\t{1}\n").format(kluch, slovnik[kluch])
                zapis2.write(jjj)
        print("Создан частотный словарь")
    finally:
        zapis.close()
    try:
        for kluch in sortklucha:
            if kluch in stopslova:
                pass
            else:
                jjj = str("{0}\t{1}\n").format(kluch, slovnik[kluch])
                zapis2.write(jjj)
        print("Создан частотный словарь со стоп-словами")
    finally:
        zapis2.close()


#csvwww = open("csvdict1.csv", "w", encoding='utf-8')
#csvwww.write("Artist" + "\t" + "Album" + "\t" + "Song" + "\n") 

#csvwww.close()

if __name__ == '__main__':
    main()