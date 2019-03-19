import { Component, OnInit, Output, EventEmitter, Inject, OnDestroy } from '@angular/core';
import { FormGroup, FormControl, Validators, AbstractControl } from '@angular/forms';
import { Observable, combineLatest, ReplaySubject, Subscription } from 'rxjs';
import { BridgeService, WalletService, etheriumAddressValidator } from '../../../../../core';
import { DEFAULT_TRANSFER_FEE } from '../../../../../tokens';
import { map, withLatestFrom, take } from 'rxjs/operators';

@Component({
  selector: 'lto-wallet-withdraw-form',
  templateUrl: './withdraw-form.component.html',
  styleUrls: ['./withdraw-form.component.scss']
})
export class WithdrawFormComponent implements OnInit, OnDestroy {
  @Output() close = new EventEmitter();

  withdrawForm!: FormGroup;

  step: 'input' | 'confirm' | 'done' = 'input';

  confirmed = false;
  captchaResponse = '';

  transfer$: Promise<any> | null = null;

  burnRatePct$!: Observable<number>;
  burnedTokens$ = new ReplaySubject<number>(1);
  receiving$!: Observable<number>;

  BRIDGE_MINIMAL_FEE = 2.25;

  maxAmount = 0;

  private _subscriptions = new Subscription();

  get cannotSend(): boolean {
    return !this.confirmed || !this.captchaResponse;
  }

  constructor(
    private _wallet: WalletService,
    private _bridge: BridgeService,
    @Inject(DEFAULT_TRANSFER_FEE) private _transferFee: number
  ) {}

  ngOnInit() {
    this.burnRatePct$ = this._bridge.burnRate$.pipe(map(rate => rate * 100));

    this.withdrawForm = new FormGroup({
      amount: new FormControl(
        this.BRIDGE_MINIMAL_FEE,
        [Validators.min(this.BRIDGE_MINIMAL_FEE), Validators.required],
        this.validateAmount.bind(this)
      ),
      address: new FormControl('', [Validators.required, etheriumAddressValidator])
    });

    this._subscriptions.add(
      this.withdrawForm.valueChanges
        .pipe(
          map(value => value.amount),
          withLatestFrom(this._bridge.burnRate$),
          map(([amount, burnRate]) => {
            const burned = amount * burnRate;
            return burned < this.BRIDGE_MINIMAL_FEE ? this.BRIDGE_MINIMAL_FEE : burned;
          })
        )
        .subscribe(this.burnedTokens$)
    );

    this.receiving$ = this.withdrawForm.valueChanges.pipe(
      map(value => value.amount),
      withLatestFrom(this.burnedTokens$),
      map(([amount, burned]) => {
        return amount - burned;
      })
    );

    this._subscriptions.add(
      combineLatest(this._wallet.balance$, this._wallet.transferFee$).subscribe(
        ([balance, transferFee]) => {
          this.maxAmount =
            (balance.available - transferFee * balance.amountDivider) / balance.amountDivider;
        }
      )
    );
  }

  ngOnDestroy() {
    this._subscriptions.unsubscribe();
  }

  goToInputStep() {
    this.withdrawForm.enable();
    this.step = 'input';
    this.captchaResponse = '';
    this.confirmed = false;
  }

  goToConfirmation() {
    this.step = 'confirm';
    this.withdrawForm.disable();
  }

  solveCaptcha(response: string) {
    this.captchaResponse = response;
  }

  confirm() {
    this.confirmed = true;
  }

  transfer() {
    const { amount, address } = this.withdrawForm.value;
    this.transfer$ = this._wallet.withdraw(
      address,
      amount,
      this._transferFee,
      this.captchaResponse
    );
  }

  closeClick() {
    this.close.next();
  }

  isInvalid(controlName: string) {
    const control = this.withdrawForm.controls[controlName];
    return control.dirty && control.invalid;
  }

  validateAmount(ctrl: AbstractControl) {
    return combineLatest(this._wallet.balance$, this._wallet.transferFee$).pipe(
      map(([balance, transferFee]) => {
        const amount = ctrl.value * balance.amountDivider;
        const maxAmount = balance.available - transferFee * balance.amountDivider;
        const invalid = amount > maxAmount;
        return invalid ? { max: true } : null;
      }),
      take(1)
    );
  }

  getFormErrors(): string[] {
    const errors = [];
    const amountCtrl = this.withdrawForm.controls.amount;
    const addressCtrl = this.withdrawForm.controls.address;
    if (amountCtrl.dirty && amountCtrl.errors) {
      if (amountCtrl.errors.min) {
        errors.push('Amount cannot be less than ' + this.BRIDGE_MINIMAL_FEE);
      }

      if (amountCtrl.errors.max) {
        errors.push('Maxumum amount is ' + this.maxAmount);
      }

      if (amountCtrl.errors.required) {
        errors.push('Amount required');
      }
    }

    if (addressCtrl.dirty && addressCtrl.errors) {
      if (addressCtrl.errors.invalidAddress) {
        errors.push('Invalid address');
      }

      if (addressCtrl.errors.required) {
        errors.push('Address required');
      }
    }

    return errors;
  }
}
