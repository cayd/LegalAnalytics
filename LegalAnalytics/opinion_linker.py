import json, re

def is_number(n):
    try:
        float(n)   # Type-casting the string to `float`.
    except ValueError:
        return False
    return True

def extract_case_file(cf):
    #print('cf', cf)
    cf = cf.split()
    for i in range(len(cf)):
        if "No" in cf[i]:
            ret = ""
            for j in range(i+1, len(cf)):
                ret += cf[j] + " "
            return ret[:-1]

def clean_regex(reg):
    for i in range(len(reg)-1, 0, -1):
        if reg[i].isalnum():
            return reg
        else:
            reg = reg[:-1]
        
def main():
    acr = []
    with open('appeals_court_opinions.json') as json_file:
        acr = json.load(json_file)
        #        for lst in data:
        #            for i in lst:
        #                print(i['title'])
        #                appeals_court_titles.append(i['title'])
        #    print(len(appeals_court_titles))

    for i in range(len(acr)):
        if acr[i] is None:
            print("None", i)
    appeals_court_records = [item for sublist in acr for item in sublist]

    print("appeals", len(appeals_court_records))

    scr = []
    with open('supreme_court_opinions.json') as json_file:
        scr = json.load(json_file)
        #        for lst in data:
        #            for i in lst:
        #                print(i['title'])
        #                supreme_court_titles.append(i['title'])
        #    print(len(supreme_court_titles))
    supreme_court_records = [item for sublist in scr for item in sublist]

    subdividers = ['CIVDS', 'RIC', 'CV', 'A', 'CR', 'CIV', 'JCP', 'CGC', 'SCN', 'BS']
    match_count = 0
    matches = 0

    pairs = []
    for rec_s in supreme_court_records:
        matches_found = 0
        reg1 = 'Super\.[ ]*Ct\.[ ]*No\.[ ]*'
        reg2 = '[^ ]*'
        use_reg = reg1 + reg2
        m = re.findall(use_reg, rec_s['opinion'])
        if m == []:
            continue
        case_file = m[0].split()[-1]
        if (not is_number(case_file)) and len(case_file) < 6:
            use_reg = reg1+case_file+'[ ]*'+reg2
            m = re.findall(use_reg, rec_s['opinion'])
            if m == []:
                continue
            case_file = extract_case_file(m[0])
        case_expr = clean_regex(reg1 + str(case_file))
        # save the match here
        saved_rec_a = None
        for rec_a in appeals_court_records:
            concurring_match = re.findall(case_expr, rec_a['opinion'])
            if not concurring_match == []:
                #print("match btwn", rec_s['title'], rec_a['title'], " with key ", case_file, " from ", m)
                matches_found += 1
                matches += 1
                saved_rec_a = rec_a
        if matches_found > 0:
            pairs.append((saved_rec_a, rec_s))
            match_count += 1
        #if matches_found == 1:

            
    print(match_count, " total matches")
    print(matches, " in total")
    print(len(pairs), "matches will be used")

    #print(pairs)
    id_num = 0
    with open('training_data.json', 'w') as data_file, open('label_data.json', 'w') as labels_file:
        training_data, label_data = {}, {}
        training_data['examples'] = []
        label_data['examples'] = []
        for pair in pairs:
            print(pair[0]['title'], pair[1]['title'])
            pair[0]['id'], pair[1]['id'] = id_num, id_num
            id_num += 1
            training_data['examples'].append(pair[0])
            label_data['examples'].append(pair[1])
        json.dump(training_data, data_file, indent=4)
        json.dump(label_data, labels_file, indent=4)
        #training_json, labels_json = json.dumps(training_data), json.dumps(labels)
        #data_file.write(training_json)
        #labels_file.write(labels_json)
        
    print("Saving is complete")
    
if __name__ == "__main__":
    main()
        
