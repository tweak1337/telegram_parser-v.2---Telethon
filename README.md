**Updated version of telegram parser, at this time based on Telethon library, which able significantly bigger, than Pyrogram at my last version.**

Now we are parsing news channels, contains property thematics.
Added 3 new fields:
1) Forwared - How many forwards does the message have.
2) Finded_agent - finded agent of my current work place, after getting the message, script goes to database to get list of agents, and trying to find one or more of them in message.text
3) clients_cnt - how many agents has been finded in previous column.

My advise, use Telethon instead of Pyrogram, especially if you want to integrate the program at your workplace system, cause here you don't even need to keep .session file, 
possible to set "string connection" (look at the connection in the begining of program), and also you can use http proxy connection.

Ps. At this project i had use only one .py file, without any session and config files.
All variables are located at virtual environment.
I suppose this is the best way to use your parser without any problems with kubernetes and docker.

python 3.10.2

