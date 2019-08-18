import json
import pprint

def main():
    labels = []
    with open('label_data.json') as json_file:
        labels = json.load(json_file)
    print("loading complete")

    for i in labels['examples']:
        try:
            pprint.pprint(i['labeled'])
            continue
        except:
            pass
        pprint.pprint(i['title'])
        pprint.pprint(i['url'])
        try:
            pprint.pprint(i['summary'])
        except:
            pass
        i['affirmed'] = 0
        i['overturned'] = 0
        i['both'] = 0
        while True:
            l = input("a for affirm, o for overturn, b for both ")
            if l == 'a':
                i['affirmed'] = 1
                break
            if l == 'o':
                i['overturned'] = 1
                break
            if l == 'b':
                i['both'] = 1
                break
            print("unrecognized input")
                
        i['labeled']=True
        
        with open('full_labels.json', 'w') as labels_file:
            json.dump(labels, labels_file, indent=4)
    
if __name__ == "__main__":
    main()
