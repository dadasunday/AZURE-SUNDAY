from voice_generator import VoiceGenerator

def main():
    generator = VoiceGenerator()
    
    # Example text to convert to speech
    text = "Hello! This is a test of the voice generation system."
    
    # Generate speech
    generator.generate_speech(text)

if __name__ == "__main__":
    main()