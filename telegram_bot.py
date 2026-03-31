import telebot

API_TOKEN = 'YOUR_API_TOKEN'
bot = telebot.TeleBot(API_TOKEN)

# Dictionary to store factory details
factories = {}

@bot.message_handler(commands=['add_factory'])
def add_factory(message):
    bot.send_message(message.chat.id, "Please send me the factory name.")
    bot.register_next_step_handler(message, process_factory_name)

def process_factory_name(message):
    factory_name = message.text
    bot.send_message(message.chat.id, "Please send me the location of the factory.")
    bot.register_next_step_handler(message, process_factory_location, factory_name)

def process_factory_location(message, factory_name):
    factory_location = message.text
    bot.send_message(message.chat.id, "Please send me the production type of the factory.")
    bot.register_next_step_handler(message, process_production_type, factory_name, factory_location)

def process_production_type(message, factory_name, factory_location):
    production_type = message.text
    # Store the factory details
    factories[factory_name] = {'location': factory_location, 'production_type': production_type}
    confirmation_message = (f"Factory '{factory_name}' added successfully!\n" +
                            f"Location: {factory_location}\n" +
                            f"Production Type: {production_type}")
    bot.send_message(message.chat.id, confirmation_message)

@bot.message_handler(commands=['view_factories'])
def view_factories(message):
    if not factories:
        bot.send_message(message.chat.id, "No factories available.")
    else:
        for name, details in factories.items():
            bot.send_message(message.chat.id, f"Name: {name}, Location: {details['location']}, Production Type: {details['production_type']}")

@bot.message_handler(commands=['start'])
def start_command(message):
    bot.send_message(message.chat.id, "Welcome to the Factory Management Bot!\nUse /add_factory to add a new factory.\nUse /view_factories to view added factories.")

if __name__ == '__main__':
    bot.polling()