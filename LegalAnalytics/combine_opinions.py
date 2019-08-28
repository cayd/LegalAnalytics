import json
from pprint import pprint

def main():
    my_labels = []
    with open('full_labels.json') as json_file:
        my_labels = json.load(json_file)
    print("loading complete")

    daves_labels = []
    with open('../../../OneDrive/Desktop/cade/daves_labels.json') as json_file:
        daves_labels = json.load(json_file)
    print("loading complete")

    #training_data = []
    #with open('../../../OneDrive/Desktop/cade/daves_labels.json') as json_file:
    #    training_data = json.load(json_file)
    #    print("loading complete")
                    
    for i in my_labels['examples']:
        try:
            print(i['labeled'])
        except:
            print("no labels")
            for j in daves_labels['examples']:
                if j['id'] == i['id']:
                    try:
                        print(j['labeled'])
                        #i = j
                        i['affirmed'] = j['affirmed']
                        i['reversed'] = j['reversed']
                        i['both'] = j['both']
                        i['labeled'] = j['labeled']
                    except:
                        pass

    affirmed, reveresed, both = 0, 0, 0
    for i in my_labels['examples']:
        try:
            print(i['affirmed'])
            affirmed += i['affirmed']
            reveresed += i['reversed']
            both += i['both']
        except:
            print("UNMATCHED BAD BAD", i['title'])

    print('a', affirmed)
    print('r', reveresed)
    print('b', both)
    with open('combined_labels.json', 'w') as labels_file:
        json.dump(my_labels, labels_file, indent=4)
            
if __name__ == "__main__":
    main()
