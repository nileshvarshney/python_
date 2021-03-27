alphabets = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']


def caeser(direction, plain_text, shift_amount):
    """
    Function is used to encrypt/ decrpt the provided string by using shift value
    Args:
        direction : accepts decrypt or encrypt
        plain_text : any string text
        shift_amount : accept any shift value
    Output:
       returns encrypted or decrypted text
    """
    new_list = []
    for ch in plain_text:
        if ch in alphabets:
            if direction == 'decrypt': 
                new_position = alphabets.index(ch) - shift_amount
                if new_position < 0:
                    new_position  += len(alphabets) 
                new_list.append(alphabets[new_position])
            elif direction == 'encrypt': 
                new_position = alphabets.index(ch)  + shift_amount
                if new_position > len(alphabets) -1:
                    new_position =  new_position - len(alphabets) 

                new_list.append(alphabets[new_position])
        else:
            new_list.append(ch)
    print(''.join(new_list))

if __name__ == "__main__":
    should_continue = True
    while should_continue:
        direction = input("Type 'encrypt' to encrypt the text, type 'decrypt' to decrypt the code\t")
        text = input(f"Enter the text to {direction}\t").lower()
        shift = int(input("Inter the number of shift\t")) % 26
        caeser(direction,text,shift)
        should_continue = input("Type 'yes' if you want to go again else type 'no'\t")
        if should_continue in ['no','No']:
            should_continue = False