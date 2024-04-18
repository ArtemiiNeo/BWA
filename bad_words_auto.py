# -*- coding: utf-8 -*-
import requests 
import pandas as pd
import seaborn as sns
import datetime
from matplotlib import pyplot as plt
from wh import wb_token

# Список активных и приостановленных кампаний
def get_active_ads(year, month, day):
    # Получаем все рекламные компании
    print('getting dict')
    
        
    # Берём только свежие
    cur_year = datetime.datetime(int(year), int(month), int(day))
    
    wb_adv_dict = requests.get(
            url = 'https://advert-api.wb.ru/adv/v1/promotion/count',
            headers =  {'Authorization':wb_token,
                        'Content-Type': 'application/json'}
            )
    
    print(wb_adv_dict)
    print(wb_adv_dict.json()['all'])
    data = wb_adv_dict.json()['adverts']

    # Выбираем кампании активные (9) и приостановленные (11)
    print('starting to select ids')
    a = []

    for i in data:
        if i['status'] in (9, 11):
            for j in i['advert_list']:
                j['status'] = i['status']
                j['type'] = i['type']
                a.append(j)
    print(f"active and paused {len(a)}")
    # Приводим дату к нужному формату
    for i in range(len(a)):
        a[i]['changeTime'] = datetime.datetime.strptime(a[i]['changeTime'].split('T')[0], '%Y-%m-%d')
    
    b = [] 

    for i in a:
        if i['changeTime'] > cur_year:
                b.append({'id': i['advertId']})
                
    print(f"recent {len(b)}")
    
    return b

# Находим и исключаем плохие ключевые слова по id АВТОМАТИЧЕСКОЙ кампании
def bad_words_auto(id):
    print(f"Работаем с {id}")    

    def get_kw_stats(id):
        data = requests.get(
                url = "https://advert-api.wb.ru/adv/v2/auto/daily-words",
                headers={'Authorization':wb_token,
                            'Content-Type': 'application/json'},
                params = {"id": id})
        c_stats = pd.json_normalize(data.json(), record_path='stat', meta = 'date')
        c_stats = c_stats.loc[c_stats['views']>0] # Очень важно! В ответе почему-то есть строки с кучей кликов и без просмотров

        c_stats = c_stats.groupby(['keyword']).agg({'views':'sum', 'clicks':'sum', 'sum':'sum'})
        c_stats = c_stats.reset_index()
        return c_stats
    
    # Исключаем ключевые фразы ..1 в 6 сек.
    def exclude_bad_words(id, words):
        exc = requests.post(
                    url = "https://advert-api.wb.ru/adv/v1/auto/set-excluded",
                    headers={'Authorization':wb_token,
                                'Content-Type': 'application/json'},
                    params = {"id": id},
                    json = {"excluded": words}
                    )
        return exc # не оч важно что-то возвращать, можно просто статус смотреть
    
    # Получаем ранее исключенные фразы и распределение текущих фраз по кластерам ..4 в 1 сек.
    def words_by_clusters(id):
        data = requests.get(url = "https://advert-api.wb.ru/adv/v2/auto/stat-words",
                        headers={'Authorization':wb_token,
                                'Content-Type': 'application/json'},
                        params = {"id": id})
        data = data.json()
        kw_by_clusters = pd.json_normalize(data['clusters']).explode('keywords') # [ [index] [cluster] [count] [keywords] 
        kw_by_clusters = kw_by_clusters.reset_index()
        kw_by_clusters.drop_duplicates(subset = 'keywords', inplace=True, keep='first', ignore_index=True)
        excluded = data['excluded'] # [excluded]
        
        return kw_by_clusters[['keywords', 'cluster']], excluded

    # Получаем кластеры, исключенные фразы, статистику по фразам
    clusters, excluded = words_by_clusters(id)

    stats = get_kw_stats(id)

    # Смотрим кластер каждого ключевого запроса (попадаются запросы без кластера, но, как правило, они почти без показов)
    for_ex = pd.merge(stats, clusters, left_on='keyword', right_on='keywords', how='left')
    grouped = for_ex.groupby('cluster').agg({'views':'sum','clicks':'sum','sum':'sum'})


    # Рассчитываем ctr и чуть меняем форматы. Берём фразы с показам более ста
    grouped['ctr'] = round(grouped['clicks']/grouped['views']*100,2)
    grouped['sum'] = round(grouped['sum'],0).astype('int64')

    grouped = grouped.loc[grouped['views']>=100]


    #SNS - график с распределением кликов и трат по ctr%

    sns.set_style('darkgrid')
    sns.kdeplot(x=grouped['ctr'],weights=grouped['sum'], fill=True, color='orange', cut=0, alpha=0.5)
    sns.kdeplot(x=grouped['ctr'],weights=grouped['clicks'], fill=True, color='green', cut=0, alpha=0.5)


    plt.show()
    # STATS

    mean_ctr = grouped.ctr.mean()
    target_ctr = mean_ctr*0.2

    print("\n\nSTATS")
    print(f'Всего кликов: {grouped.clicks.sum()}\nВсего потрачено: {grouped['sum'].sum()}')


    print("\n\nALL KEYWORDS")
    print(grouped.to_string())


    sw = ""


    # Основной цикл. Можно оставить while True
    while sw !="q":
   
        bad_words = grouped.loc[grouped['ctr']<target_ctr]

        print("\n\nBAD WORDS")
        print(bad_words.to_string())

        bw_stats = pd.DataFrame({'Всего': [int(grouped['sum'].sum()), int(grouped['clicks'].sum())],
                                 'Выбранные': [int(bad_words['sum'].sum()), int(bad_words['clicks'].sum())],
                                 'ix': ['Потрачено: ', 'Кликов: ']
                                 })
    
        bw_stats.set_index('ix', inplace = True)
        bw_stats['Доля'] = round(bw_stats['Выбранные']/bw_stats['Всего']*100,2)
    
        print('\n')
        print(bw_stats)
    
        sw = input("\n\nd:   delete\nc:   change\nq:   quit\n\n")
    
        match sw:
            case "d":
                ttl = list(list(for_ex.loc[for_ex['cluster'].isin(bad_words.index)]['keyword']))+excluded
                exclude_bad_words(id, ttl)
                print(f'XCLUDED:\n{ttl}')

                break
            case "c":
                target_ctr = float(input('Введите нижнюю границу ctr\n'))
            case "q":
                break
            case _:
                continue
    ### TO DO:
    ### 
    ### 1. Обработка всевозможных ошибок, в первую очередь ошибки запросов к апи. 
    ### 2. Дополнительные методы изменения bad_words -> [], такие как добавление\убирание отдельных запросов по имени кластера
            

            

bad_words_auto(15897343)

# adv_list = []

# import adv_list

# for i in adv_list:
#     bad_words_auto(i)


