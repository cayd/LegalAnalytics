import json
import pprint

def main():
    labels = []
    with open('full_labels.json') as json_file:
        labels = json.load(json_file)
    print("loading complete")

    labels['examples'].reverse()
    
    for i in labels['examples']:
        pprint.pprint(i['id'])
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
        i['reversed'] = 0
        i['both'] = 0
        while True:
            l = input("a for affirm, r for reverse, b for both ")
            if l == 'a':
                i['affirmed'] = 1
                break
            if l == 'r':
                i['reversed'] = 1
                break
            if l == 'b':
                i['both'] = 1
                break
            print("unrecognized input")
                
        i['labeled']=True
        
        with open('full_labels.json', 'w') as labels_file:
            labels['examples'].reverse()
            json.dump(labels, labels_file, indent=4)
            labels['examples'].reverse()
            
if __name__ == "__main__":
    main()
