from replit import clear
logo = '''
                         ___________
                         \         /
                          )_______(
                          |"""""""|_.-._,.---------.,_.-._
                          |       | | |               | | ''-.
                          |       |_| |_             _| |_..-'
                          |_______| '-' `'---------'` '-'
                          )"""""""(
                         /_________\\
                       .-------------.
                      /_______________\\
'''
print(logo)
biding = {}
bidding_open = True

def highest_bidding():
    highest_bidder_name = ''
    highest_bid_amount = 0
    for k, v in biding.items():
        if v > highest_bid_amount:
            highest_bidder_name = k
            highest_bid_amount = v
        else:
            pass
    print("*"*70) 
    print(f'Highest Bidder is {highest_bidder_name} with bidding amount {highest_bid_amount}')
    print("*"*70) 

    
if __name__ == "__main__":
    while bidding_open:
        name = input("Enter Your Name :\t")
        bid = int(input('Enter your Bid Amount :\t'))
        is_bidding_continue = input("Do you want to enter another bid(yes/no)?")
        biding[name] = bid
        if is_bidding_continue in ["no",'No','NO']:
            bidding_open = False
            highest_bidding()
        elif is_bidding_continue in ["yes",'Yes','YES']:
            clear()