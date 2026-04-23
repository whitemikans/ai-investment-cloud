Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
base = fso.GetParentFolderName(WScript.ScriptFullName)
cmd = "cmd /c """ & base & "\起動_ダッシュボード.bat"""
shell.Run cmd, 0, False
