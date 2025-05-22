import os
from dotenv import load_dotenv
from openai import OpenAI
import json

load_dotenv()  # Loads variables from .env into environment
client = OpenAI()

def get_person_prediction(first_name: str, last_name: str):
    response = client.responses.create(
        model="gpt-4.1-nano",
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "You are an expert linguist and an expert in name-based gender detection. Given the first name and last name, determine the most likely first language and the most likely gender of the person, assuming they are from South Africa. Respond with only the language name and the gender name, each on its own line, with no additional text, explanation, or formatting."
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": f"first_name: {first_name} last_name: {last_name}" # Dynamically inserting names here
                    }
                ]
            }
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "person_prediction",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "language": {
                            "type": "string",
                            "description": "Most likely first language of the person",
                            "enum": [
                                "Afrikaans",
                                "English",
                                "isiNdebele",
                                "isiXhosa",
                                "isiZulu",
                                "Sepedi",
                                "Sesotho",
                                "Setswana",
                                "siSwati",
                                "Tshivenda",
                                "Xitsonga"
                            ]
                        },
                        "gender": {
                            "type": "string",
                            "description": "Most likely gender of the person",
                            "enum": [
                                "FEMALE",
                                "MALE"
                            ]
                        },
                        "confidence": {
                            "type": "number",
                            "description": "The prediction score indicating the confidence level of the prediction"
                        }
                    },
                    "required": [
                        "language",
                        "gender",
                        "confidence"
                    ],
                    "additionalProperties": False
                }
            }
        },
        reasoning={},
        tools=[],
        temperature=1,
        max_output_tokens=2048,
        top_p=1,
        store=True
    )
    return response

def print_prediction_with_metadata(first_name, last_name):
    prediction_response = get_person_prediction(first_name, last_name)
    prediction_json = prediction_response.output[0].content[0].text
    prediction = json.loads(prediction_json)
    # Add model and token info
    prediction["model"] = prediction_response.model
    prediction["input_tokens"] = prediction_response.usage.input_tokens
    prediction["output_tokens"] = prediction_response.usage.output_tokens

    # Pricing for gpt-4.1-nano-2025-04-14
    input_token_price = 0.0000001
    output_token_price = 0.0000004

    input_cost = prediction["input_tokens"] * input_token_price
    output_cost = prediction["output_tokens"] * output_token_price
    prediction["input_cost_usd"] = round(input_cost, 8)
    prediction["output_cost_usd"] = round(output_cost, 8)
    prediction["total_cost_usd"] = round(input_cost + output_cost, 8)

    # Calculate what it would cost if this usage was scaled to 1M calls
    prediction["cost_per_1M_calls_at_this_usage"] = {
        "input_usd": round(prediction["input_cost_usd"] * 1_000_000, 2),
        "output_usd": round(prediction["output_cost_usd"] * 1_000_000, 2),
        "total_usd": round(prediction["total_cost_usd"] * 1_000_000, 2)
    }

    print(json.dumps(prediction, indent=2))

# Example usage:
print_prediction_with_metadata("Kyle", "van der Westhuizen")
print_prediction_with_metadata("Nomusa", "Dlamini")
print_prediction_with_metadata("John", "Smith")
