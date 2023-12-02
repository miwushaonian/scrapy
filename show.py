import cv2

cap = cv2.VideoCapture("./a.mp4")
fpsn = 0
while True:
    ret, frame = cap.read()
    if ret == False:
        break
    fpsn = fpsn + 1
    if fpsn == 4008:
        cv2.imshow("a", frame)
        cv2.waitKey(0)
