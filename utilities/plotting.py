import io
from PIL import Image

def Plt2Pil(fig) -> Image:
    buffer = io.BytesIO()
    fig.savefig(buffer, format='png', bbox_inches='tight', pad_inches=0)
    buffer.seek(0)
    return Image.open(buffer)
