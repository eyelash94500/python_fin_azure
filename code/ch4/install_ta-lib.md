# Windoes 安裝 TA-Lib

與 Mac OS 相同，TA-Lib 無法直接用 `pip` 或是 `pipenv`，如果有安裝`Anaconda`的話，就包含在裡面，但是筆者的做法是需要才要安裝，因此無法直接安裝 TA-Lib。

> 注：此篇對應書本的 p.128~129

## 直接安裝會失敗

如果直接使用 `pip` 或是 `pipenv`安裝（`pipenv install TA-Lib`），會出現中的錯誤：

>error: Microsoft Visual C++ 14.0 is required. Get it with "Microsoft Visual C++ Build Tools": https://visualstudio.microsoft.com/downloads/

或是：

> × Encountered error while trying to install package.
> 
> ╰─> ta-lib
> 
> note: This is an issue with the package mentioned above, not pip.
> 
> hint: See above for output from the failure.
>
> Installation Failed

這時候，可以同書籍中 Mac OS 的方式，下載檔案後進行安裝。

## 下載 .whl 檔與安裝 C++ 開發套件

這邊會參考[官網提供的資訊](https://github.com/mrjbq7/ta-lib#windows)，需要下載一份 .whl 檔案，並且取得 C++ 開發套件後即可安裝。

1. 取得 .whl 檔案

首先進入網站：https://github.com/mrjbq7/ta-lib#windows，根據自己的作業系統版本，取得檔案。筆者環境是 Windows 10 64位元，因此下載：

```
TA_Lib-0.4.24-cp310-cp310-win_amd64.whl
```

![TA-Lib 列表](pic/talib%20list.png)

2. 安裝 C++ 開發套件

在這邊需要安裝一些開發相關的套件，可以藉由安裝 Visual Studio Community 然後從中選擇「Visual C++」相關的資訊，進行安裝就可以取得套件。

筆者這邊是下載 Visual Studio Community 2022，並且勾選「C++桌面開發」內容（可參考畫面內容，或是直接下載檔案進行安裝也行）。

![install C++](pic/install%20c++.png)

3. 安裝 TA-Lib

上面兩步設定完後，就可以進行 TA-Lib 的安裝，直接使用所下載的檔案進行安裝，語法為`pipenv install {檔案路徑}`：

```
pipenv install TA_Lib-0.4.24-cp310-cp310-win_amd64.whl
```

接著就會自行安裝，這時候在`Pipfile`就會產生一筆紀錄，說安裝 TA-Lib 的路徑在哪裡（本範例是放在根目錄下面）。當安裝完畢就是大功告成囉！

![pipenv lisg](pic/pipenv%20list.png)

4. 測試 TA-Lib

安裝完成後，就進行測試，可以使用範例檔案－[install_ta-lib.ipynb](install_ta-lib.ipynb)。裡面有使用 ta-lib 的`SMA()` 功能，看著圖有符合**SMA**的值與樣式，就表示 TA-Lib 可以順利運作。