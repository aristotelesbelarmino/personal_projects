import requests
import os
import pandas as pd
auth_params = {
    'key': 'KEY STRING',
    'token': 'TOKEN STRING'
}

board_id = 'TRELLO BOARD ID'
card_id = 'TRELLO CARD ID'

def reading_csv():
    dataframe = pd.read_csv(r'.csv FILE')
    return dataframe

def test():
    text:str = ''
    my_list = reading_csv()

    for i in my_list.iterrows():
        text = f"{text} + {i}"
    return text
def reading_board(auth_params, board_id):
    lists_response = requests.get(
        f'https://api.trello.com/1/boards/{board_id}/lists',
        params=auth_params
    )

    lists = lists_response.json()
    for list in lists:
        cards_response = requests.get(
            f'https://api.trello.com/1/lists/{list["id"]}/cards',
            params=auth_params
        )
        cards = cards_response.json()

        for card in cards:
            card_response = requests.get(
                f'https://api.trello.com/1/cards/{card["id"]}',
                params=auth_params
            )
            card_content = card_response.json()["name"]
            print(card_content)

#reading_board(auth_params, board_id)

def write_on_card(content):
    
    url = f'https://api.trello.com/1/cards/{card_id}/actions/comments'
    data = {
        'text': content,
        **auth_params
    }

    response = requests.post(url, data=data)
    if response.ok:
        print('Comment added successfully!')
    else:
        print('Error adding comment:', response.text)

write_on_card(test())