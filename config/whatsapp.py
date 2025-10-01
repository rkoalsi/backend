import plivo
import os
from dotenv import load_dotenv
from plivo.utils.template import Template  # Import the Template class

load_dotenv()

PLIVO_AUTH_ID = os.getenv("PLIVO_AUTH_ID")
PLIVO_AUTH_TOKEN = os.getenv("PLIVO_AUTH_TOKEN")
FROM_NUMBER = os.getenv("FROM_NUMBER")
client = plivo.RestClient(PLIVO_AUTH_ID, PLIVO_AUTH_TOKEN)


def generate_whatsapp_template(template_doc: dict, dynamic_params: dict) -> Template:
    """
    Given a template document from MongoDB and a dictionary of dynamic parameters,
    generate a Plivo Template instance with a body component and optionally a button component.

    For example, if dynamic_params is:
      {
          "name": "Rohan",
          "order_id": "12345",
          "button_url": "https://your-dynamic-url.com"
      }

    The generated components will be:
      [
          {
              "type": "body",
              "parameters": [
                  {"type": "text", "text": "Rohan"},
                  {"type": "text", "text": "12345"}
              ]
          },
          {
              "type": "button",
              "sub_type": "url",
              "index": "0",
              "parameters": [
                  {"type": "text", "text": "https://your-dynamic-url.com"}
              ]
          }
      ]

    You can adjust this logic to support more buttons or different structures.
    """
    components = []

    # Prepare body parameters: exclude button-related keys (like 'button_url')
    body_params = [
        {"type": "text", "text": str(value)}
        for key, value in dynamic_params.items()
        if key != "button_url"
    ]

    # Only add a body component if we have body parameters
    if body_params:
        components.append(
            {
                "type": "body",
                "parameters": body_params,
            }
        )

    # Check if there's a dynamic URL for a button
    button_url = dynamic_params.get("button_url")
    if button_url:
        button_component = {
            "type": "button",
            "sub_type": "url",  # for URL button
            "index": "0",  # change this index if you have multiple buttons
            "parameters": [{"type": "text", "text": str(button_url)}],
        }
        components.append(button_component)

    # Return a Plivo Template instance using the collected components
    return Template(
        name=template_doc.get("name"),
        language=template_doc.get("language"),
        components=components,
    )


def send_whatsapp(to: str, template_doc: dict, params: dict):
    try:
        cleaned_phone = ''.join(char for char in str(to) if char.isdigit())
        
        if not cleaned_phone:
            raise ValueError(f"Invalid phone number after cleaning: {to}")
        
        response = client.messages.create(
            type_="whatsapp",
            src=FROM_NUMBER,
            dst=f"+91{cleaned_phone}",
            template=generate_whatsapp_template(template_doc, params),
        )
        return response
    except plivo.exceptions.AuthenticationError as e:
        print("Authentication failed:", e)
    except Exception as e:
        print("An error occurred:", e)
