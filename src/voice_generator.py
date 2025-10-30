import os
from gtts import gTTS
import pyttsx3
from dotenv import load_dotenv

class VoiceGenerator:
    def __init__(self):
        # Initialize text-to-speech engine
        self.engine = pyttsx3.init()
        
        # Load environment variables
        load_dotenv()
        
    def generate_speech(self, text, output_file="output.mp3", use_gtts=False):
        """
        Generate speech from text using either pyttsx3 (offline) or gTTS (online).
        
        Args:
            text (str): Text to convert to speech
            output_file (str): Output audio file name
            use_gtts (bool): Whether to use Google Text-to-Speech (requires internet)
        """
        try:
            if use_gtts:
                # Use Google Text-to-Speech (requires internet connection)
                tts = gTTS(text=text, lang='en')
                tts.save(output_file)
                print(f"Speech generated and saved to {output_file}")
            else:
                # Use pyttsx3 (offline text-to-speech)
                self.engine.say(text)
                self.engine.runAndWait()
                print("Speech generated using offline engine")
                
        except Exception as e:
            print(f"Error generating speech: {str(e)}")
    
    def set_voice_properties(self, rate=None, volume=None):
        """
        Set voice properties for pyttsx3 engine
        
        Args:
            rate (int): Speaking rate (words per minute)
            volume (float): Volume (0.0 to 1.0)
        """
        if rate:
            self.engine.setProperty('rate', rate)
        if volume:
            self.engine.setProperty('volume', volume)