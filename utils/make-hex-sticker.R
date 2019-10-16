library(hexSticker)

#imgurl <- "docs/reference/figures/disk.frame.png"
imgurl <- "inst/figures/logo.svg"

sticker(imgurl, package="disk.frame", p_size=20, s_x=1, s_y=.75, s_width=.6,
        filename="inst/figures/hex-logo.png", h_fill = "black", p_color = "white", h_color = "dark blue")

sticker(
  imgurl, 
  package="disk.frame", 
  p_size=80, 
  p_y = 1.5,
  s_x=1, 
  s_y=.75, 
  s_width=.6,
  filename="inst/figures/hex-logo-1200.png", 
  h_fill = "black", 
  p_color = "white", 
  h_color = "dark blue", 
  dpi=1200)

