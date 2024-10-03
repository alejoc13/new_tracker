import helper.controler as op



if __name__ == "__main__":
    menu = True
    while menu != False:
        try:
            option = input('''
            1. Tracker por CFN
            2. Tracker por SubOU
            3. Tracker por Número de Registro
            4. Tracker de planning review(fechas omitidas de submitted y Approved)
            5. Tracker de procesos Resagados(mas tiempo del esperado en Submitted)
            6. Tracker Missed Voucher
            7. Tracker por lapsos de tiempo(se da una fecha de inicio y de finalización)
            8. Tracker de elementos vencidos(Se omite Honduras y Rep. Dominicanda)
            9. Missed Critical communications(Cancell Renewal)
            10. Missed Critical communications (approved CFN Withdrawal)
            11. Gaps on DataBases Report
            12. Tracker by CFN (as sufix)
            13. Funcion experimental comparacion difusa
            14. Reportar Expected Submission Date omitidas
            15. Track no ID licenses
            16.External tracker for no ID
            17.Compare Expirations Dates (SP and DBs)
            18. Review Approval Dates
            Presione entre sin ningun texto para salir
            ingrese el número de la opción a utilizar: ''')
            option =int(option)
        except:
            if option == '':
                menu = False
                break
            else:
                print('Opción invalida')
                print('#-------------------------------')
        if option == 1:
            op.Option1()
        elif option == 2:
            op.Option2()
        elif option == 3:
            op.Option3()
        # elif option == 4:
        #     op.option_Planning()
        # elif option == 5:
        #     op.option_submitted()
        # elif option == 6:
        #     op.option_voucher()
        elif option == 7:
            op.Option7()
        elif option == 8:
            op.Option8()         
        # elif option == 9:
        #     op.cancel_criticalOP()           
        # elif option == 10:
        #     op.CFN_WithdrawalOP()
        # elif option == 11:
        #     op.gaps_option()
        elif option == 12:
            op.Option12()
        elif option == 13:
            op.Option13()
        # elif option == 14:
        #     op.MissedExpectedDAte()
        # elif option == 15:
        #     op.planingRenewals()
        # elif option == 16:
        #     op.compareNoID()
        # elif option == 17:
        #     op.DatesComparation()
        # elif option == 18:
        #     op.approvalsReview()
            
        else:
            print('Opción incorrecta')
            print('#-------------------------------')
    
